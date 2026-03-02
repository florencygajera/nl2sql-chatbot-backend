"""Test script for universal database connector module."""

import sys
from app.utils.universal_db_connector import UniversalConnectionManager, test_connection

# Test various connection string formats
test_cases = [
    # ADO.NET SQL Server
    ('ADO.NET MSSQL', 'Data Source=localhost,1433;Initial Catalog=MyDB;User ID=myuser;Password=mypass;', 'mssql'),
    # PostgreSQL URL
    ('PostgreSQL URL', 'postgresql://myuser:mypass@localhost:5432/mydb', 'postgres'),
    # MySQL URL
    ('MySQL URL', 'mysql://myuser:mypass@localhost:3306/mydb', 'mysql'),
    # SQLite
    ('SQLite', 'sqlite:///test.db', 'sqlite'),
]

manager = UniversalConnectionManager()

print("=" * 60)
print("Universal Database Connector Test")
print("=" * 60)
print()

for name, conn_str, db_type in test_cases:
    try:
        print(f"Testing: {name}")
        print(f"  Input: {conn_str}")
        
        params = manager.parse_and_validate(
            connection_string=conn_str, 
            db_type=db_type
        )
        
        # Convert to SQLAlchemy URL
        url = manager.parser.to_sqlalchemy_url(params)
        
        print(f"  DB Type: {params.db_type.value}")
        print(f"  Host: {params.host}, Port: {params.port}, DB: {params.database}")
        print(f"  Username: {params.username}")
        print(f"  SQLAlchemy URL: {url}")
        print(f"  Result: SUCCESS")
        print()
        
    except Exception as e:
        print(f"  Result: ERROR - {e}")
        import traceback
        traceback.print_exc()
        print()

print("=" * 60)
print("Test Complete")
print("=" * 60)
