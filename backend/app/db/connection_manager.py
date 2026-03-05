def _check_sql_server_driver_available() -> None:
    import pyodbc
    drivers = [d.lower() for d in pyodbc.drivers()]
    if not any("odbc driver 17 for sql server" in d or "odbc driver 18 for sql server" in d for d in drivers):
        raise ValueError(
            "SQL Server ODBC driver not found. Install 'ODBC Driver 17/18 for SQL Server' on Windows."
        )