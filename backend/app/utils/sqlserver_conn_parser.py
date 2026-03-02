"""
SQL Server connection string parser utility.

Parses SQL Server connection strings in ADO.NET format and converts them
to SQLAlchemy URLs.
"""

from typing import Tuple, Dict, Any
import urllib.parse


def parse_sqlserver_connection_string(connection_string: str) -> Tuple[Dict[str, Any], str]:
    """
    Parse a SQL Server connection string and return parsed parameters and SQLAlchemy URL.

    Supports formats like:
    - Data Source=host,Port;Initial Catalog=db;User ID=user;Password=pass;Trusted_connection=True;TrustServerCertificate=True;Integrated Security=False;
    - Server=host,Port;Database=db;User Id=user;Password=pass;TrustServerCertificate=yes;
    - Data Source=host;Port=1433;Initial Catalog=db;User ID=user;Password=pass;

    Args:
        connection_string: SQL Server connection string in ADO.NET format

    Returns:
        Tuple of (parsed_params_dict, sqlalchemy_url_string)

    Raises:
        ValueError: If required parameters are missing or invalid
    """
    # Initialize result dictionary with defaults
    parsed: Dict[str, Any] = {
        "host": None,
        "port": None,
        "database": None,
        "username": None,
        "password": None,
        "TrustServerCertificate": True,
        "Encrypt": False,  # FIXED: Default to False to avoid prelogin handshake errors
        "Integrated_Security": False,
    }

    # Normalize and split the connection string
    conn_str = connection_string.strip()
    parts = [p.strip() for p in conn_str.split(";") if p.strip()]

    # IMPORTANT: If "Integrated Security" is explicitly present, do not let Trusted_Connection override it.
    seen_integrated_security = False

    for part in parts:
        if "=" not in part:
            continue

        key, _, value = part.partition("=")
        key = key.strip().lower()
        value = value.strip()

        # Handle Data Source / Server
        if key in ("data source", "server", "datasource"):
            if "," in value:
                # Format: host,port
                host_port = value.split(",")
                parsed["host"] = host_port[0].strip()
                if len(host_port) > 1:
                    try:
                        parsed["port"] = int(host_port[1].strip())
                    except ValueError:
                        pass
            elif "\\" in value:
                # Format: host\instance (named instance)
                parsed["host"] = value
                parsed["port"] = None  # Named instance uses dynamic port unless configured
            else:
                parsed["host"] = value

        # Handle separate Port key
        elif key == "port":
            try:
                parsed["port"] = int(value)
            except ValueError:
                pass

        # Handle Initial Catalog / Database
        elif key in ("initial catalog", "database"):
            parsed["database"] = value

        # Handle User ID / User / UID
        elif key in ("user id", "user", "uid"):
            parsed["username"] = value

        # Handle Password
        elif key == "password":
            parsed["password"] = value

        # Handle Integrated Security
        elif key == "integrated security":
            parsed["Integrated_Security"] = value.lower() in ("true", "yes", "sspi")
            seen_integrated_security = True

        # Handle Trusted_connection (ONLY if Integrated Security was not explicitly set)
        elif key == "trusted_connection":
            if not seen_integrated_security:
                parsed["Integrated_Security"] = value.lower() in ("true", "yes", "sspi")

        # Handle TrustServerCertificate
        elif key in ("trustservercertificate", "trust server certificate"):
            parsed["TrustServerCertificate"] = value.lower() in ("true", "yes")

        # Handle Encrypt
        elif key == "encrypt":
            parsed["Encrypt"] = value.lower() in ("true", "yes")

    # If Integrated Security is True, clear username/password (Windows Authentication)
    if parsed["Integrated_Security"]:
        parsed["username"] = None
        parsed["password"] = None

    # Build SQLAlchemy URL
    if not parsed["host"]:
        raise ValueError("Host is required. Could not find 'Data Source' or 'Server' in connection string.")

    if not parsed["database"]:
        raise ValueError("Database is required. Could not find 'Initial Catalog' or 'Database' in connection string.")

    host_part = parsed["host"]
    if parsed["port"]:
        host_part = f"{host_part},{parsed['port']}"

    database = urllib.parse.quote_plus(parsed["database"])

    # CRITICAL FIX: Default Encrypt to "no" to prevent prelogin handshake failures.
    # The original code defaulted to Encrypt=yes, which causes:
    #   [08001] Client unable to establish connection because an error was
    #   encountered during handshakes before login
    # on servers that don't support TLS or have self-signed certs.
    encrypt = "yes" if parsed.get("Encrypt", False) else "no"
    tsc = "yes" if parsed.get("TrustServerCertificate", True) else "no"

    # Handle Integrated Security / Windows Authentication (supported by pyodbc)
    if parsed["Integrated_Security"]:
        return parsed, (
            f"mssql+pyodbc://{host_part}/{database}"
            f"?driver=ODBC+Driver+17+for+SQL+Server"
            f"&Encrypt={encrypt}"
            f"&TrustServerCertificate={tsc}"
            f"&Trusted_Connection=yes"
        )

    # Require username and password for SQL Authentication
    if not parsed["username"] or not parsed["password"]:
        raise ValueError(
            "Username and Password are required for SQL Server connection when Integrated Security is false."
        )

    # URL-encode special characters in username and password
    username = urllib.parse.quote_plus(parsed["username"])
    password = urllib.parse.quote_plus(parsed["password"])

    sqlalchemy_url = (
        f"mssql+pyodbc://{username}:{password}@{host_part}/{database}"
        f"?driver=ODBC+Driver+17+for+SQL+Server"
        f"&Encrypt={encrypt}"
        f"&TrustServerCertificate={tsc}"
    )

    return parsed, sqlalchemy_url


# Example usage and testing
if __name__ == "__main__":
    # NOTE: This example is intentionally "conflicting". Integrated Security=False should win,
    # so it will require SQL user/password.
    test_connection_string = (
        "Data Source=localhost,1433; Initial Catalog=MyDB; User ID=myuser; Password=mypass; "
        "Trusted_connection=True; TrustServerCertificate=True; Integrated Security=False;"
    )

    print("Testing connection string:")
    print(f"  Input: {test_connection_string}")
    print()

    try:
        parsed_params, sqlalchemy_url = parse_sqlserver_connection_string(test_connection_string)

        print("Parsed Parameters:")
        for key, value in parsed_params.items():
            print(f"  {key}: {value}")

        print()
        print(f"SQLAlchemy URL: {sqlalchemy_url}")

    except ValueError as e:
        print(f"Error: {e}")

    print()
    print("-" * 60)
    print()

    test2 = "Server=192.168.1.100,1433; Database=TestDB; User Id=sa; Password=P@ssw0rd; Encrypt=True; TrustServerCertificate=True;"
    print(f"Testing: {test2}")
    try:
        parsed2, url2 = parse_sqlserver_connection_string(test2)
        print(f"  Parsed: host={parsed2['host']}, port={parsed2['port']}, db={parsed2['database']}, user={parsed2['username']}")
        print(f"  URL: {url2}")
    except ValueError as e:
        print(f"  Error: {e}")