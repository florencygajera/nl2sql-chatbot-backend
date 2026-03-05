"""
Direct pyodbc connection test.
"""

import pyodbc

# Actual server details
SERVER = "6gd6btjtnbtux3fhcs6psyufz1ec6gyh3i8hfmkpcmhwdtvwuozaxu8uoptlcsk.printftech.com"
PORT = "2408"
DATABASE = "DB_GramBook_v11.0"
USERNAME = "Demo"
PASSWORD = "sa@123"

print(f"Testing connection to {SERVER},{PORT}")
print("=" * 60)

# Build connection string - using ODBC Driver 17
# IMPORTANT: Encrypt=no prevents prelogin handshake failures
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER},{PORT};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    f"TrustServerCertificate=yes;"
    f"Encrypt=no;"
)

print(f"Connection string: {conn_str}")
print("-" * 60)

try:
    conn = pyodbc.connect(conn_str, timeout=15)
    print("SUCCESS! Connected!")
    
    cursor = conn.cursor()
    cursor.execute("SELECT @@VERSION")
    row = cursor.fetchone()
    print(f"SQL Server Version: {row[0][:100]}...")
    
    # List tables
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    tables = cursor.fetchall()
    print(f"\nFound {len(tables)} tables:")
    for t in tables[:20]:
        print(f"  - {t[0]}")
    
    cursor.close()
    conn.close()
    print("\nConnection test PASSED!")
    
except pyodbc.Error as e:
    print(f"\nFAILED!")
    print(f"Error code: {e.args[0]}")
    print(f"Message: {e.args[1]}")
except Exception as e:
    print(f"\nERROR: {e}")
