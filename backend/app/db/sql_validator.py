"""
SQL Validation Layer using sqlglot

This module provides SQL validation using sqlglot for:
- Syntax validation
- Table/column existence checking
- Query type validation (SELECT only)
- SQL security validation
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.dialects import dialect

logger = logging.getLogger(__name__)


# ============== Dialect Mapping ==============

def get_sqlglot_dialect(db_dialect: str) -> Optional[str]:
    """Map database dialect to sqlglot dialect name."""
    dialect_map = {
        "postgresql": "postgres",
        "postgres": "postgres",
        "mssql": "tsql",
        "mysql": "mysql",
        "sqlite": "sqlite",
        "oracle": "oracle"
    }
    return dialect_map.get(db_dialect.lower())


# ============== Validation Result ==============

@dataclass
class SQLValidationResult:
    """Result of SQL validation."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    parsed_sql: Optional[exp.Expression] = None
    referenced_tables: List[str] = field(default_factory=list)
    referenced_columns: List[str] = field(default_factory=list)
    query_type: str = ""
    has_limit: bool = False
    limit_value: Optional[int] = None


# ============== Validation Functions ==============

def validate_sql_syntax(
    sql: str,
    db_dialect: str = "postgres"
) -> Tuple[bool, str, Optional[exp.Expression]]:
    """
    Validate SQL syntax using sqlglot.
    
    Returns:
        Tuple of (is_valid, error_message, parsed_expression)
    """
    if not sql or not sql.strip():
        return False, "Empty SQL query", None
    
    glot_dialect = get_sqlglot_dialect(db_dialect)
    
    try:
        # Try to parse with specific dialect
        if glot_dialect:
            parsed = sqlglot.parse(sql, dialect=glot_dialect)
        else:
            # Try without specific dialect
            parsed = sqlglot.parse(sql)
        
        if not parsed or parsed[0] is None:
            return False, "Failed to parse SQL", None
        
        return True, "", parsed[0]
        
    except Exception as e:
        return False, f"Syntax error: {str(e)}", None


def check_query_type(parsed: exp.Expression) -> Tuple[str, bool]:
    """
    Check what type of query this is.
    
    Returns:
        Tuple of (query_type, is_select)
    """
    query_type = "unknown"
    is_select = False
    
    # Check root expression
    if isinstance(parsed, exp.Select):
        query_type = "SELECT"
        is_select = True
    elif isinstance(parsed, exp.Insert):
        query_type = "INSERT"
    elif isinstance(parsed, exp.Update):
        query_type = "UPDATE"
    elif isinstance(parsed, exp.Delete):
        query_type = "DELETE"
    elif isinstance(parsed, exp.Create):
        query_type = "CREATE"
    elif isinstance(parsed, exp.Drop):
        query_type = "DROP"
    elif isinstance(parsed, exp.Alter):
        query_type = "ALTER"
    elif isinstance(parsed, exp.Truncate):
        query_type = "TRUNCATE"
    elif isinstance(parsed, exp.Merge):
        query_type = "MERGE"
    elif isinstance(parsed, exp.Union):
        query_type = "UNION"
        is_select = True
    elif isinstance(parsed, exp.Except):
        query_type = "EXCEPT"
    elif isinstance(parsed, exp.Intersect):
        query_type = "INTERSECT"
    
    return query_type, is_select


def extract_referenced_tables(parsed: exp.Expression) -> List[str]:
    """Extract all table references from the SQL."""
    tables = set()
    
    # Walk the AST
    for node in parsed.walk():
        if isinstance(node, exp.Table):
            table_name = node.name
            if table_name and not table_name.startswith('_'):
                tables.add(table_name.lower())
    
    return sorted(list(tables))


def extract_referenced_columns(parsed: exp.Expression) -> List[str]:
    """Extract all column references from the SQL."""
    columns = set()
    
    for node in parsed.walk():
        if isinstance(node, exp.Column):
            col_name = node.name
            if col_name and not col_name.startswith('_'):
                columns.add(col_name.lower())
        elif isinstance(node, exp.Alias):
            alias = node.alias
            if alias:
                columns.add(alias.lower())
    
    return sorted(list(columns))


def check_limit_clause(parsed: exp.Expression, db_dialect: str) -> Tuple[bool, Optional[int]]:
    """Check if LIMIT clause exists and get its value."""
    # Check for LIMIT
    limit_node = parsed.find(exp.Limit)
    if limit_node:
        # Get the limit value
        if limit_node.expression:
            if isinstance(limit_node.expression, exp.Literal):
                try:
                    return True, int(limit_node.expression.name)
                except (ValueError, AttributeError):
                    pass
        return True, None
    
    # Check for TOP (MSSQL)
    if db_dialect.lower() == "mssql":
        top_node = parsed.find(exp.Top)
        if top_node:
            if top_node.expression:
                if isinstance(top_node.expression, exp.Limit):
                    limit_val = top_node.expression
                    if isinstance(limit_val, exp.Literal):
                        try:
                            return True, int(limit_val.name)
                        except (ValueError, AttributeError):
                            pass
                    elif isinstance(limit_val, exp.Parameter):
                        return True, None
    
    return False, None


def validate_table_references(
    sql_tables: List[str],
    schema_tables: List[str],
    allow_join_tables: bool = True
) -> Tuple[bool, List[str]]:
    """
    Validate that referenced tables exist in schema.
    
    Returns:
        Tuple of (is_valid, error_messages)
    """
    if not schema_tables:
        return True, []  # No schema to validate against
    
    schema_table_set = set(t.lower() for t in schema_tables)
    errors = []
    
    for table in sql_tables:
        table_lower = table.lower()
        
        # Check exact match
        if table_lower not in schema_table_set:
            # Try without schema prefix
            parts = table_lower.split('.')
            if len(parts) > 1:
                short_name = parts[-1]
                if short_name not in schema_table_set:
                    errors.append(f"Unknown table: {table}")
            else:
                errors.append(f"Unknown table: {table}")
    
    return len(errors) == 0, errors


def validate_column_references(
    sql_columns: List[str],
    table_columns: Dict[str, Set[str]]
) -> Tuple[bool, List[str]]:
    """
    Validate that referenced columns exist in tables.
    
    Args:
        sql_columns: Columns referenced in SQL
        table_columns: Dict of table -> set of column names
        
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    # Get all columns from all tables
    all_columns: Set[str] = set()
    for cols in table_columns.values():
        all_columns.update(cols)
    
    # For now, just check if column exists in any table
    # (Full validation would need column-to-table mapping)
    for col in sql_columns:
        col_lower = col.lower()
        if col_lower not in all_columns:
            # Allow some common functions
            if col_lower not in {'count', 'sum', 'avg', 'min', 'max', 
                                  'upper', 'lower', 'trim', 'now', 'date',
                                  'year', 'month', 'day', 'coalesce', 'null'}:
                errors.append(f"Unknown column: {col}")
    
    return len(errors) == 0, errors


# ============== Main Validator ==============

class SQLValidator:
    """Main SQL validation class."""
    
    def __init__(
        self,
        schema_tables: Optional[List[str]] = None,
        table_columns: Optional[Dict[str, Set[str]]] = None,
        db_dialect: str = "postgres",
        max_rows: int = 1000
    ):
        self.schema_tables = schema_tables or []
        self.table_columns = table_columns or {}
        self.db_dialect = db_dialect
        self.max_rows = max_rows
        
    def validate(self, sql: str) -> SQLValidationResult:
        """
        Validate SQL query.
        
        Returns:
            SQLValidationResult with detailed validation info
        """
        errors = []
        warnings = []
        
        # 1. Check for forbidden keywords (safety)
        forbidden_found = self._check_forbidden_keywords(sql)
        if forbidden_found:
            return SQLValidationResult(
                is_valid=False,
                errors=[f"Forbidden SQL keyword: {forbidden_found}"]
            )
        
        # 2. Parse and validate syntax
        is_valid, parse_error, parsed = validate_sql_syntax(sql, self.db_dialect)
        if not is_valid:
            return SQLValidationResult(
                is_valid=False,
                errors=[parse_error]
            )
        
        # 3. Check query type (must be SELECT)
        query_type, is_select = check_query_type(parsed)
        if not is_select:
            return SQLValidationResult(
                is_valid=False,
                errors=[f"Only SELECT queries are allowed, got: {query_type}"],
                query_type=query_type
            )
        
        # 4. Extract table and column references
        sql_tables = extract_referenced_tables(parsed)
        sql_columns = extract_referenced_columns(parsed)
        
        # 5. Validate table references
        tables_valid, table_errors = validate_table_references(
            sql_tables, 
            self.schema_tables
        )
        if not tables_valid:
            errors.extend(table_errors)
        
        # 6. Validate column references
        if self.table_columns and sql_columns:
            cols_valid, col_errors = validate_column_references(
                sql_columns,
                self.table_columns
            )
            if not cols_valid:
                warnings.extend(col_errors)
        
        # 7. Check for multiple statements (semicolons)
        if ';' in sql.strip().rstrip(';'):
            warnings.append("Multiple statements detected")
        
        # 8. Check LIMIT clause
        has_limit, limit_val = check_limit_clause(parsed, self.db_dialect)
        if not has_limit:
            warnings.append(f"No LIMIT clause - will be capped at {self.max_rows}")
        
        # 9. Check for dangerous patterns
        dangerous = self._check_dangerous_patterns(sql)
        if dangerous:
            errors.append(f"Dangerous pattern detected: {dangerous}")
        
        return SQLValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            parsed_sql=parsed,
            referenced_tables=sql_tables,
            referenced_columns=sql_columns,
            query_type=query_type,
            has_limit=has_limit,
            limit_value=limit_val
        )
    
    def _check_forbidden_keywords(self, sql: str) -> Optional[str]:
        """Check for forbidden SQL keywords."""
        forbidden = {
            'INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER',
            'TRUNCATE', 'CREATE', 'REPLACE', 'MERGE', 'CALL',
            'EXEC', 'EXECUTE', 'GRANT', 'REVOKE', 'COMMIT',
            'ROLLBACK', 'SAVEPOINT', 'COPY', 'LOAD', 'IMPORT'
        }
        
        sql_upper = sql.upper()
        
        for keyword in forbidden:
            # Use word boundaries to avoid false matches
            pattern = r'\b' + keyword + r'\b'
            if re.search(pattern, sql_upper):
                return keyword
        
        return None
    
    def _check_dangerous_patterns(self, sql: str) -> Optional[str]:
        """Check for dangerous SQL patterns."""
        # Comment injection
        if '--' in sql or '/*' in sql or '*/' in sql:
            return "SQL comments not allowed"
        
        # Multiple statements
        if sql.count(';') > 1:
            return "Multiple statements not allowed"
        
        # UNION with non-select
        if re.search(r'\bUNION\s+(ALL\s+)?SELECT', sql, re.IGNORECASE):
            pass  # UNION is okay
        
        return None
    
    def inject_limit(self, sql: str) -> str:
        """
        Inject LIMIT clause if not present.
        
        Uses dialect-appropriate syntax.
        """
        if self.db_dialect.lower() == "mssql":
            # MSSQL uses TOP
            if "TOP" not in sql.upper():
                sql = sql.replace("SELECT", "SELECT TOP " + str(self.max_rows), 1)
        elif self.db_dialect.lower() == "oracle":
            # Oracle uses FETCH FIRST
            if "FETCH" not in sql.upper():
                sql = sql.rstrip().rstrip(';')
                if not sql.upper().endswith("FETCH FIRST"):
                    sql = sql + f" FETCH FIRST {self.max_rows} ROWS ONLY"
        else:
            # PostgreSQL, MySQL, SQLite use LIMIT
            if "LIMIT" not in sql.upper():
                sql = sql.rstrip().rstrip(';')
                sql = sql + f" LIMIT {self.max_rows}"
        
        return sql


def validate_and_fix_sql(
    sql: str,
    schema_tables: Optional[List[str]] = None,
    table_columns: Optional[Dict[str, Set[str]]] = None,
    db_dialect: str = "postgres",
    max_rows: int = 1000
) -> Tuple[str, SQLValidationResult]:
    """
    Validate and optionally fix SQL.
    
    Returns:
        Tuple of (processed_sql, validation_result)
    """
    validator = SQLValidator(
        schema_tables=schema_tables,
        table_columns=table_columns,
        db_dialect=db_dialect,
        max_rows=max_rows
    )
    
    result = validator.validate(sql)
    
    if not result.is_valid:
        return sql, result
    
    # Fix missing LIMIT
    if not result.has_limit:
        sql = validator.inject_limit(sql)
        result.has_limit = True
        result.limit_value = max_rows
    
    return sql, result