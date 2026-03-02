"""
Universal Database Connection Module.

This module provides comprehensive support for parsing and connecting to databases
using various connection string formats including:
- PostgreSQL (SQLAlchemy URL, environment variables)
- MySQL (SQLAlchemy URL, standard connection strings)
- Oracle (SQLAlchemy URL, TNS-less connection strings)
- SQLite (file paths, SQLAlchemy URLs)
- SQL Server / MSSQL (ADO.NET, ODBC, pyodbc connection strings)
- ODBC connection strings
- OLE DB connection strings
- JDBC-style connection strings (for Java compatibility)
- Provider-specific formats

Features:
- Automatic database type detection
- Connection string validation
- Support for both full server details and direct connection strings
- Proper error handling with descriptive messages
- Connection testing before establishing
"""

from __future__ import annotations

import os
import re
import urllib.parse
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Tuple, Union
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


# Database type enumeration
class DatabaseType(Enum):
    """Supported database types."""
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    MSSQL = "mssql"
    ORACLE = "oracle"
    UNKNOWN = "unknown"


# Default ports for each database type
DEFAULT_PORTS: Dict[DatabaseType, int] = {
    DatabaseType.POSTGRESQL: 5432,
    DatabaseType.MYSQL: 3306,
    DatabaseType.MSSQL: 1433,
    DatabaseType.ORACLE: 1521,
    DatabaseType.SQLITE: 0,
}


@dataclass
class ConnectionParams:
    """Parsed connection parameters."""
    db_type: DatabaseType
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    driver: Optional[str] = None
    provider: Optional[str] = None
    sslmode: Optional[str] = None
    trust_server_certificate: bool = True
    integrated_security: bool = False
    encrypt: bool = False  # Default to False to avoid prelogin handshake errors
    file_path: Optional[str] = None
    additional_params: Dict[str, str] = field(default_factory=dict)

    @property
    def is_file_based(self) -> bool:
        """Check if this is a file-based database (SQLite)."""
        return self.db_type == DatabaseType.SQLITE or self.file_path is not None


class ConnectionStringError(Exception):
    """Raised when connection string is invalid or cannot be parsed."""
    pass


class ConnectionError(Exception):
    """Raised when connection to database fails."""
    pass


class UniversalConnectionParser:
    """
    Universal connection string parser that handles multiple formats.

    Supported formats:
    - SQLAlchemy URLs: postgresql://user:pass@host:port/db
    - ADO.NET: Data Source=host;Initial Catalog=db;User ID=user;Password=pass
    - ODBC: Driver={ODBC Driver 17 for SQL Server};Server=host;Database=db;UID=user;PWD=pass
    - JDBC: jdbc:postgresql://host:port/db?user=user&password=pass
    - Key-Value: host=host;port=port;database=db;user=user;password=pass
    - Full details: host, port, database, username, password
    """

    # Database type detection patterns
    DB_TYPE_PATTERNS = {
        DatabaseType.POSTGRESQL: [
            r"^postgresql",
            r"^postgres",
            r"jdbc:postgresql",
        ],
        DatabaseType.MYSQL: [
            r"^mysql",
            r"^mariadb",
            r"jdbc:mysql",
        ],
        DatabaseType.MSSQL: [
            r"^mssql",
            r"^sqlserver",
            r"^sql server",
            r"jdbc:sqlserver",
            r"jdbc:sql",
        ],
        DatabaseType.ORACLE: [
            r"^oracle",
            r"jdbc:oracle",
        ],
        DatabaseType.SQLITE: [
            r"^sqlite",
        ],
    }

    # SQLAlchemy dialect mappings
    SQLALCHEMY_DIALECTS = {
        DatabaseType.POSTGRESQL: "postgresql+psycopg2",
        DatabaseType.MYSQL: "mysql+pymysql",
        DatabaseType.MSSQL: "mssql+pyodbc",
        DatabaseType.ORACLE: "oracle+oracledb",
        DatabaseType.SQLITE: "sqlite",
    }

    # ODBC driver mappings
    ODBC_DRIVERS = {
        DatabaseType.POSTGRESQL: "PostgreSQL Unicode",
        DatabaseType.MYSQL: "MySQL ODBC 8.0 Driver",
        DatabaseType.MSSQL: "ODBC Driver 17 for SQL Server",
        DatabaseType.ORACLE: "Oracle in OraDBHome19c",
    }

    def __init__(self):
        self._db_type: Optional[DatabaseType] = None

    def detect_db_type(self, connection_string: str) -> DatabaseType:
        """
        Detect database type from connection string.
        """
        conn_lower = connection_string.lower().strip()

        # Check for SQLAlchemy URL format
        if "://" in conn_lower:
            for db_type, patterns in self.DB_TYPE_PATTERNS.items():
                for pattern in patterns:
                    if re.match(pattern, conn_lower):
                        return db_type

        # Check for JDBC format
        if conn_lower.startswith("jdbc:"):
            if "oracle" in conn_lower:
                return DatabaseType.ORACLE
            elif "sqlserver" in conn_lower or "sql" in conn_lower:
                return DatabaseType.MSSQL
            elif "mysql" in conn_lower or "mariadb" in conn_lower:
                return DatabaseType.MYSQL
            elif "postgres" in conn_lower:
                return DatabaseType.POSTGRESQL

        # Check for ADO.NET / ODBC / Key-Value format
        conn_str_lower = conn_lower

        # Check for SQL Server specific keys
        if any(k in conn_str_lower for k in ["data source", "server", "initial catalog", "database"]):
            if any(k in conn_str_lower for k in ["user id", "password", "integrated security", "trusted_connection"]):
                return DatabaseType.MSSQL

        if "initial catalog" in conn_str_lower or "data source" in conn_str_lower:
            return DatabaseType.MSSQL

        if ".sqlite" in conn_lower or ".db" in conn_lower or ".sqlite3" in conn_lower:
            return DatabaseType.SQLITE

        if "sslmode" in conn_str_lower or "ssl" in conn_str_lower:
            return DatabaseType.POSTGRESQL

        return DatabaseType.UNKNOWN

    def parse_key_value_string(self, connection_string: str) -> Dict[str, str]:
        """
        Parse key-value format connection string.
        """
        result: Dict[str, str] = {}
        conn_str = connection_string.strip()

        parts = []
        current = ""
        in_quotes = False
        quote_char = None

        for char in conn_str:
            if char in ('"', "'") and not in_quotes:
                in_quotes = True
                quote_char = char
                current += char
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = None
                current += char
            elif char == ';' and not in_quotes:
                if current.strip():
                    parts.append(current.strip())
                current = ""
            else:
                current += char

        if current.strip():
            parts.append(current.strip())

        for part in parts:
            if "=" not in part:
                continue

            key, _, value = part.partition("=")
            key = key.strip().lower()
            value = value.strip().strip('"').strip("'")

            key_mapping = {
                "data source": "server",
                "datasource": "server",
                "addr": "server",
                "address": "server",
                "network address": "server",
                "initial catalog": "database",
                "dbname": "database",
                "user id": "user",
                "uid": "user",
                "username": "user",
                "password": "password",
                "pwd": "password",
                "port": "port",
                "ssl mode": "sslmode",
                "sslmode": "sslmode",
                "trust server certificate": "trustservercertificate",
                "trustservercertificate": "trustservercertificate",
                "integrated security": "integratedsecurity",
                "trusted_connection": "integratedsecurity",
                "driver": "driver",
                "provider": "provider",
                "encrypt": "encrypt",
            }

            key = key_mapping.get(key, key)
            result[key] = value

        return result

    def parse(
        self,
        connection_string: Optional[str] = None,
        db_type: Optional[Union[str, DatabaseType]] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs
    ) -> ConnectionParams:
        """
        Parse connection parameters from various input formats.
        """
        self._db_type = None

        if db_type is not None:
            if isinstance(db_type, str):
                db_type_lower = db_type.lower()
                if "postgres" in db_type_lower or "pg" in db_type_lower:
                    self._db_type = DatabaseType.POSTGRESQL
                elif "mysql" in db_type_lower or "mariadb" in db_type_lower:
                    self._db_type = DatabaseType.MYSQL
                elif "mssql" in db_type_lower or "sqlserver" in db_type_lower or "sql server" in db_type_lower:
                    self._db_type = DatabaseType.MSSQL
                elif "oracle" in db_type_lower:
                    self._db_type = DatabaseType.ORACLE
                elif "sqlite" in db_type_lower:
                    self._db_type = DatabaseType.SQLITE
                else:
                    self._db_type = DatabaseType.UNKNOWN
            else:
                self._db_type = db_type

        if connection_string:
            conn_str = connection_string.strip()

            if conn_str.lower().startswith("jdbc:"):
                return self._parse_jdbc_url(conn_str)

            if "://" in conn_str:
                return self._parse_sqlalchemy_url(conn_str)

            return self._parse_key_value(
                conn_str,
                host=host,
                port=port,
                database=database,
                username=username,
                password=password,
                **kwargs
            )

        if host or database or username:
            return self._build_from_params(
                host=host,
                port=port,
                database=database,
                username=username,
                password=password,
                **kwargs
            )

        raise ConnectionStringError(
            "Insufficient connection information. Provide either a connection string "
            "or individual connection parameters (host, database, etc.)"
        )

    def _parse_sqlalchemy_url(self, url: str) -> ConnectionParams:
        parsed = urllib.parse.urlparse(url)
        scheme = parsed.scheme.lower()

        if "postgres" in scheme:
            db_type = DatabaseType.POSTGRESQL
        elif "mysql" in scheme:
            db_type = DatabaseType.MYSQL
        elif "mssql" in scheme or "sqlserver" in scheme:
            db_type = DatabaseType.MSSQL
        elif "oracle" in scheme:
            db_type = DatabaseType.ORACLE
        elif "sqlite" in scheme:
            db_type = DatabaseType.SQLITE
        else:
            db_type = DatabaseType.UNKNOWN

        username = parsed.username
        password = parsed.password
        host = parsed.hostname
        port = parsed.port
        database = parsed.path.lstrip("/") if parsed.path else None
        query_params = dict(urllib.parse.parse_qsl(parsed.query))

        if (not database or database == "") and "service_name" in query_params:
            database = query_params["service_name"]

        if db_type == DatabaseType.SQLITE and database:
            if url.startswith("sqlite:///"):
                file_path = url.replace("sqlite:///", "")
            else:
                file_path = database
        else:
            file_path = None

        return ConnectionParams(
            db_type=db_type,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            file_path=file_path,
            additional_params=query_params,
        )

    def _parse_jdbc_url(self, url: str) -> ConnectionParams:
        url_lower = url.lower()

        if self._db_type and self._db_type != DatabaseType.UNKNOWN:
            db_type = self._db_type
        elif "postgres" in url_lower:
            db_type = DatabaseType.POSTGRESQL
        elif "mysql" in url_lower or "mariadb" in url_lower:
            db_type = DatabaseType.MYSQL
        elif "oracle" in url_lower:
            db_type = DatabaseType.ORACLE
        elif "sqlserver" in url_lower:
            db_type = DatabaseType.MSSQL
        else:
            db_type = DatabaseType.UNKNOWN

        stripped_url = re.sub(r'^jdbc:', '', url, flags=re.IGNORECASE)
        parsed = urllib.parse.urlparse(stripped_url)

        host = parsed.hostname
        port = parsed.port
        database = parsed.path.lstrip("/") if parsed.path else ""

        query_params = dict(urllib.parse.parse_qsl(parsed.query))
        username = query_params.pop("user", None)
        password = query_params.pop("password", None)

        return ConnectionParams(
            db_type=db_type,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            additional_params=query_params,
        )

    def _parse_key_value(
        self,
        connection_string: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs
    ) -> ConnectionParams:
        if self._db_type is None:
            self._db_type = self.detect_db_type(connection_string)

        params = self.parse_key_value_string(connection_string)

        raw_server = params.get("server") or params.get("host")
        host = host or raw_server

        if port:
            pass
        elif "port" in params:
            try:
                port = int(params["port"])
            except (ValueError, TypeError):
                pass
        elif raw_server and "," in raw_server:
            server_parts = raw_server.split(",")
            if len(server_parts) > 1:
                try:
                    port = int(server_parts[1].strip())
                    host = server_parts[0].strip()
                except (ValueError, TypeError):
                    pass

        if host and "," in host:
            parts = host.split(",")
            host = parts[0].strip()
            if not port and len(parts) > 1:
                try:
                    port = int(parts[1].strip())
                except (ValueError, TypeError):
                    pass

        database = database or params.get("database")
        username = username or params.get("user")
        password = password or params.get("password")

        trust_server_certificate = params.get("trustservercertificate", "").lower() in ("yes", "true", "1")

        integrated_security = params.get("integratedsecurity", "").lower() in ("yes", "true", "sspi", "1")
        # If integrated security is on, ignore user/pass
        if integrated_security:
            username = None
            password = None

        # Parse encrypt flag — default to False for MSSQL to avoid prelogin handshake errors
        encrypt_val = params.get("encrypt", "").lower()
        encrypt = encrypt_val in ("yes", "true", "1")

        sslmode = params.get("sslmode")
        driver = params.get("driver")

        # If still unknown, try to infer from available parameters
        db_type = self._db_type
        if db_type == DatabaseType.UNKNOWN:
            if database and (database.endswith(".sqlite") or database.endswith(".db")):
                db_type = DatabaseType.SQLITE
            elif username and password:
                db_type = DatabaseType.POSTGRESQL

        file_path = None
        if db_type == DatabaseType.SQLITE:
            file_path = database or host

        return ConnectionParams(
            db_type=db_type,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            driver=driver,
            sslmode=sslmode,
            trust_server_certificate=trust_server_certificate,
            integrated_security=integrated_security,
            encrypt=encrypt,
            file_path=file_path,
            additional_params=params,
        )

    def _build_from_params(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs
    ) -> ConnectionParams:
        db_type = self._db_type or DatabaseType.UNKNOWN
        file_path = None
        if db_type == DatabaseType.SQLITE:
            file_path = database or host

        return ConnectionParams(
            db_type=db_type,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            file_path=file_path,
        )

    def to_sqlalchemy_url(self, params: ConnectionParams) -> str:
        db_type = params.db_type

        if params.port is None and db_type != DatabaseType.SQLITE:
            params.port = DEFAULT_PORTS.get(db_type)

        if db_type == DatabaseType.SQLITE:
            if params.file_path:
                return f"sqlite:///{params.file_path}"
            elif params.database:
                return f"sqlite:///{params.database}"
            else:
                return "sqlite:///:memory:"

        if db_type == DatabaseType.MSSQL:
            return self._build_mssql_url(params)
        elif db_type == DatabaseType.POSTGRESQL:
            return self._build_postgresql_url(params)
        elif db_type == DatabaseType.MYSQL:
            return self._build_mysql_url(params)
        elif db_type == DatabaseType.ORACLE:
            return self._build_oracle_url(params)
        else:
            dialect = self.SQLALCHEMY_DIALECTS.get(db_type, "postgresql")
            return self._build_generic_url(params, dialect)

    def _build_mssql_url(self, params: ConnectionParams) -> str:
        """Build MSSQL SQLAlchemy URL with proper TLS/encryption settings."""
        host_part = params.host or "localhost"
        if params.port:
            host_part = f"{host_part},{params.port}"

        database = quote_plus(params.database or "")

        # CRITICAL: Use Encrypt=no by default to avoid prelogin handshake failures
        # on servers that don't support TLS or have self-signed certs.
        # Only encrypt if explicitly requested AND TrustServerCertificate is set.
        encrypt = "yes" if params.encrypt else "no"
        trust = "yes" if params.trust_server_certificate else "no"

        if params.integrated_security:
            return (
                f"mssql+pyodbc://{host_part}/{database}"
                f"?driver=ODBC+Driver+17+for+SQL+Server"
                f"&Encrypt={encrypt}"
                f"&TrustServerCertificate={trust}"
                f"&Trusted_Connection=yes"
            )

        username = quote_plus(params.username or "")
        password = quote_plus(params.password or "")

        return (
            f"mssql+pyodbc://{username}:{password}@{host_part}/{database}"
            f"?driver=ODBC+Driver+17+for+SQL+Server"
            f"&Encrypt={encrypt}"
            f"&TrustServerCertificate={trust}"
        )

    def _build_postgresql_url(self, params: ConnectionParams) -> str:
        username = quote_plus(params.username or "")
        password = quote_plus(params.password or "")
        host = params.host or "localhost"
        port = params.port or 5432
        database = params.database or ""

        url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
        if params.sslmode:
            url += f"?sslmode={params.sslmode}"
        return url

    def _build_mysql_url(self, params: ConnectionParams) -> str:
        username = quote_plus(params.username or "")
        password = quote_plus(params.password or "")
        host = params.host or "localhost"
        port = params.port or 3306
        database = params.database or ""
        return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"

    def _build_oracle_url(self, params: ConnectionParams) -> str:
        username = params.username or ""
        password = params.password or ""
        host = params.host or "localhost"
        port = params.port or 1521
        database = params.database or ""
        return f"oracle+oracledb://{username}:{password}@{host}:{port}/?service_name={database}"

    def _build_generic_url(self, params: ConnectionParams, dialect: str) -> str:
        username = quote_plus(params.username or "")
        password = quote_plus(params.password or "")
        host = params.host or "localhost"
        port = params.port or 5432
        database = params.database or ""
        return f"{dialect}://{username}:{password}@{host}:{port}/{database}"


class UniversalConnectionManager:
    """
    Universal database connection manager with validation and error handling.
    """

    def __init__(self):
        self.parser = UniversalConnectionParser()
        self._engine: Optional[Engine] = None
        self._current_params: Optional[ConnectionParams] = None

    @property
    def is_connected(self) -> bool:
        return self._engine is not None

    @property
    def current_url(self) -> Optional[str]:
        if self._current_params is None:
            return None

        url = self.parser.to_sqlalchemy_url(self._current_params)

        if "@" in url:
            try:
                scheme, rest = url.split("://", 1)
                if "@" in rest:
                    creds, host_part = rest.split("@", 1)
                    if ":" in creds:
                        user = creds.split(":")[0]
                        return f"{scheme}://{user}:***@{host_part}"
            except Exception:
                pass

        return url

    def parse_and_validate(
        self,
        connection_string: Optional[str] = None,
        db_type: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs
    ) -> ConnectionParams:
        try:
            params = self.parser.parse(
                connection_string=connection_string,
                db_type=db_type,
                host=host,
                port=port,
                database=database,
                username=username,
                password=password,
                **kwargs
            )
            self._validate_params(params)
            return params
        except Exception as e:
            if isinstance(e, ConnectionStringError):
                raise
            raise ConnectionStringError(f"Failed to parse connection string: {str(e)}")

    def _validate_params(self, params: ConnectionParams) -> None:
        db_type = params.db_type

        if db_type == DatabaseType.SQLITE:
            return

        if not params.host:
            raise ConnectionStringError(
                "Host is required. Please provide either a connection string "
                "or individual host parameter."
            )

        if not params.integrated_security:
            if not params.username:
                raise ConnectionStringError(
                    "Username is required for database connection. "
                    "Use integrated_security=true for Windows Authentication (MSSQL)."
                )
            if not params.password:
                raise ConnectionStringError(
                    "Password is required for database connection. "
                    "Use integrated_security=true for Windows Authentication (MSSQL)."
                )

        if not params.database:
            raise ConnectionStringError(
                "Database name is required. Please provide a database name."
            )

    def test_connection(
        self,
        params: ConnectionParams,
        timeout: int = 30
    ) -> Tuple[bool, str]:
        try:
            url = self.parser.to_sqlalchemy_url(params)

            engine_options: Dict[str, Any] = {
                "pool_pre_ping": True,
                "echo": False,
            }

            if params.db_type != DatabaseType.SQLITE:
                # MSSQL (pyodbc) uses 'timeout', Postgres commonly uses 'connect_timeout'
                if params.db_type == DatabaseType.MSSQL:
                    engine_options["connect_args"] = {"timeout": max(timeout, 30)}
                else:
                    engine_options["connect_args"] = {"connect_timeout": timeout}
            else:
                engine_options["connect_args"] = {"check_same_thread": False}

            test_engine = create_engine(url, **engine_options)

            with test_engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            test_engine.dispose()
            return True, "Connection successful"

        except SQLAlchemyError as e:
            error_msg = str(e).split("\n")[0]
            return False, f"Connection failed: {error_msg}"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    def connect(
        self,
        connection_string: Optional[str] = None,
        db_type: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        test_connection: bool = True,
        timeout: int = 30,
        **kwargs
    ) -> Tuple[Engine, ConnectionParams]:

        params = self.parse_and_validate(
            connection_string=connection_string,
            db_type=db_type,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            **kwargs
        )

        if test_connection:
            success, message = self.test_connection(params, timeout)
            if not success:
                raise ConnectionError(message)

        url = self.parser.to_sqlalchemy_url(params)

        engine_options: Dict[str, Any] = {
            "pool_pre_ping": True,
            "echo": False,
        }

        if params.db_type == DatabaseType.SQLITE:
            engine_options["connect_args"] = {"check_same_thread": False}
            self._engine = create_engine(url, **engine_options)
            self._current_params = params
            return self._engine, params

        # Pool settings for server DBs
        engine_options.update({
            "pool_size": 5,
            "max_overflow": 10,
            "pool_timeout": 30,
            "pool_recycle": 1800,
        })

        # DB-specific connect_args
        if params.db_type == DatabaseType.MSSQL:
            engine_options["connect_args"] = {"timeout": max(timeout, 30)}
        elif params.db_type == DatabaseType.POSTGRESQL:
            engine_options["connect_args"] = {
                "connect_timeout": timeout,
                "options": "-c statement_timeout=30000",
            }
        else:
            engine_options["connect_args"] = {"connect_timeout": timeout}

        self._engine = create_engine(url, **engine_options)
        self._current_params = params

        return self._engine, params

    def disconnect(self) -> None:
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._current_params = None

    def get_current_params(self) -> Optional[ConnectionParams]:
        return self._current_params


# Global instance for convenience
_default_manager = UniversalConnectionManager()


def parse_connection_string(
    connection_string: Optional[str] = None,
    db_type: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    **kwargs
) -> Tuple[ConnectionParams, str]:
    manager = UniversalConnectionManager()
    params = manager.parse_and_validate(
        connection_string=connection_string,
        db_type=db_type,
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        **kwargs
    )

    url = manager.parser.to_sqlalchemy_url(params)
    return params, url


def test_connection(
    connection_string: Optional[str] = None,
    db_type: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    timeout: int = 10,
    **kwargs
) -> Tuple[bool, str]:
    manager = UniversalConnectionManager()

    try:
        params = manager.parse_and_validate(
            connection_string=connection_string,
            db_type=db_type,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            **kwargs
        )
        return manager.test_connection(params, timeout)

    except ConnectionStringError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Error: {str(e)}"


def create_connection(
    connection_string: Optional[str] = None,
    db_type: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    test_connection: bool = True,
    timeout: int = 10,
    **kwargs
) -> Tuple[Engine, ConnectionParams]:
    manager = UniversalConnectionManager()

    return manager.connect(
        connection_string=connection_string,
        db_type=db_type,
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        test_connection=test_connection,
        timeout=timeout,
        **kwargs
    )