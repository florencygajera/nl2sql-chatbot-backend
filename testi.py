from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=6gd6btjtnbtux3fhcs6psyufz1ec6gyh3i8hfmkpcmhwdtvwuozaxu8uoptlcsk.printftech.com,2408;"
    "DATABASE=DB_GramBook_v11.0;"
    "UID=Demo;"
    "PWD=sa@123;"
    "Trusted_Connection=No;"
    "TrustServerCertificate=Yes;"
    "Encrypt=Yes;"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}",
    fast_executemany=True
)

# Test connection
with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print("✅ Connected Successfully!")