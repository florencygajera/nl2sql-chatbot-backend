"""Test full connection flow."""
import sys
sys.path.insert(0, '.')

from app.db.session import set_database_url, get_schema_summary

url = 'mssql+pyodbc://Demo:sa%40123@6gd6btjtnbtux3fhcs6psyufz1ec6gyh3i8hfmkpcmhwdtvwuozaxu8uoptlcsk.printftech.com,2408/DB_GramBook_v11.0?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes'

print("Setting database URL...")
set_database_url(url)
print("URL set successfully!")

print("Getting schema...")
schema = get_schema_summary()
print(f"Schema length: {len(schema)}")
print(f"First 300 chars:\n{schema[:300]}")
