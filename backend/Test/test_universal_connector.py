"""Test script for universal database connector module."""

import sys
from app.utils.universal_db_connector import UniversalConnectionManager, test_connection

# Test various connection string formats
test_cases = [
    # ADO.NET SQL Server
    ('ADO.NET MSSQL', 'Data Source=localhost,1433;Initial Catalog=MyDB;User ID=myuser;Password=mypass;', 'mssql'),
    # ADO.NET SQL Server (Server= format)
    ('ADO.NET MSSQL v2', 'Server=192.168.1.100,1433;Database=TestDB;User Id=sa;Password=P@ssw0rd', 'mssql'),
    # PostgreSQL URL
    ('PostgreSQL URL', 'postgresql://myuser:mypass@localhost:5432/mydb', 'postgres'),
    # PostgreSQL with psycopg2
    ('PostgreSQL psycopg2', 'postgresql+psycopg2://myuser:mypass@localhost:5432/mydb', 'postgres'),
    # MySQL URL
    ('MySQL URL', 'mysql://myuser:mypass@localhost:3306/mydb', 'mysql'),
    # MySQL with pymysql
    ('MySQL pymysql', 'mysql+pymysql://myuser:mypass@localhost:3306/mydb', 'mysql'),
    # SQLite
    ('SQLite', 'sqlite:///test.db', 'sqlite'),
    # SQLite in-memory
    ('SQLite memory', 'sqlite:///:memory:', 'sqlite'),
    # ODBC style
    ('ODBC MSSQL', 'Driver={ODBC Driver 17 for SQL Server};Server=localhost;Database=MyDB;UID=myuser;PWD=mypass;', 'mssql'),
    # JDBC PostgreSQL
    ('JDBC PostgreSQL', 'jdbc:postgresql://localhost:5432/mydb?user=myuser&password=mypass', 'postgres'),
    # Oracle URL
    ('Oracle URL', 'oracle+oracledb://myuser:mypass@localhost:1521/?service_name=mydb', 'oracle'),
]

# Test with individual parameters
param_test_cases = [
    ('PG params', {'db_type': 'postgres', 'host': 'localhost', 'port': 5432, 'database': 'mydb', 'username': 'user', 'password': 'pass'}),
    ('MySQL params', {'db_type': 'mysql', 'host': 'localhost', 'port': 3306, 'database': 'mydb', 'username': 'user', 'password': 'pass'}),
    ('MSSQL params', {'db_type': 'mssql', 'host': 'localhost', 'port': 1433, 'database': 'mydb', 'username': 'sa', 'password': 'pass'}),
    ('Oracle params', {'db_type': 'oracle', 'host': 'localhost', 'port': 1521, 'database': 'mydb', 'username': 'user', 'password': 'pass'}),
    ('SQLite params', {'db_type': 'sqlite', 'database': 'test.db'}),
]

manager = UniversalConnectionManager()

print("=" * 70)
print("Universal Database Connector Test - Connection String Formats")
print("=" * 70)
print()

passed = 0
failed = 0

for name, conn_str, db_type in test_cases:
    try:
        params = manager.parse_and_validate(
            connection_string=conn_str, 
            db_type=db_type
        )
        
        # Convert to SQLAlchemy URL
        url = manager.parser.to_sqlalchemy_url(params)
        
        print(f"  [OK] {name}")
        print(f"    DB Type: {params.db_type.value} | Host: {params.host} | Port: {params.port} | DB: {params.database}")
        print(f"    URL: {url}")
        print()
        passed += 1
        
    except Exception as e:
        print(f"  [FAIL] {name}: {e}")
        import traceback
        traceback.print_exc()
        print()
        failed += 1

print()
print("=" * 70)
print("Universal Database Connector Test - Individual Parameters")
print("=" * 70)
print()

for name, kwargs in param_test_cases:
    try:
        params = manager.parse_and_validate(**kwargs)
        url = manager.parser.to_sqlalchemy_url(params)
        
        print(f"  [OK] {name}")
        print(f"    DB Type: {params.db_type.value} | Host: {params.host} | Port: {params.port} | DB: {params.database}")
        print(f"    URL: {url}")
        print()
        passed += 1
        
    except Exception as e:
        print(f"  [FAIL] {name}: {e}")
        print()
        failed += 1

print("=" * 70)
print(f"Results: {passed} passed, {failed} failed, {passed + failed} total")
print("=" * 70)

# Test db_routes integration
print()
print("Testing db_routes integration...")
try:
    from app.api.db_routes import build_database_url
    
    # Test with connection string
    url = build_database_url(
        connection_string="Data Source=localhost,1433;Initial Catalog=TestDB;User ID=sa;Password=pass;",
        db_type="mssql",
        host=None, port=None, database=None, username=None, password=None
    )
    print(f"  [OK] build_database_url with ADO.NET: {url}")
    
    # Test with individual params
    url2 = build_database_url(
        connection_string=None,
        db_type="postgres",
        host="localhost", port=5432, database="mydb", username="user", password="pass"
    )
    print(f"  [OK] build_database_url with params: {url2}")
    
    print()
    print("db_routes integration: OK")
except Exception as e:
    print(f"  [FAIL] db_routes integration failed: {e}")
    import traceback
    traceback.print_exc()
