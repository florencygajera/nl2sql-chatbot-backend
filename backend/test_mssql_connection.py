"""
Quick test script to debug MSSQL connection issues.
Run this directly to see detailed error messages.
"""

import pyodbc

# Replace these with your actual connection details
SERVER = "your_server_ip"  # or hostname
PORT = "2408"
DATABASE = "your_database"
USERNAME = "your_username"
PASSWORD = "your_password"

# Test different connection string formats
test_conn_strings = [
    # Format 1: Comma-separated host:port
    f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER},{PORT};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;",
    
    # Format 2: With IP address
    f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER},{PORT};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;",
    
    # Format 3: Without port (default 1433 - will fail)
    f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;",
    
    # Format 4: With Encrypt/SSL disabled
    f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER},{PORT};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};Encrypt=no;TrustServerCertificate=yes;",
]

print(f"Testing connection to {SERVER},{PORT}")
print("=" * 60)

for i, conn_str in enumerate(test_conn_strings, 1):
    print(f"\nTest {i}: {conn_str[:80]}...")
    try:
        conn = pyodbc.connect(conn_str, timeout=10)
        print(f"  SUCCESS!")
        conn.close()
        break
    except pyodbc.Error as e:
        print(f"  FAILED: {e}")
    except Exception as e:
        print(f"  ERROR: {e}")

print("\n" + "=" * 60)
print("Test complete.")
