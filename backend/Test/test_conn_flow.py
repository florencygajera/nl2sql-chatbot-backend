"""
Diagnostic: trace every step of the MSSQL connect flow to find the slow one.
Run from: d:\nl2sql-chatbot-backend\backend
"""
import sys, os, time, logging

# Suppress all logging so output is clean
logging.disable(logging.CRITICAL)

os.chdir(os.path.dirname(os.path.abspath(__file__)))

CONN_STR = (
    "Data Source=6gd6btjtnbtux3fhcs6psyufz1ec6gyh3i8hfmkpcmhwdtvwuozaxu8uoptlcsk.printftech.com,2408;"
    "Initial Catalog=DB_GramBook_v11.0;"
    "User ID=Demo;"
    "Password=sa@123;"
    "TrustServerCertificate=True;"
    "Integrated Security=False;"
)

results = []

def timed(label, fn):
    t = time.time()
    try:
        result = fn()
        elapsed = time.time() - t
        status = "OK" if elapsed < 5 else "SLOW"
        results.append(f"[{status}] {label}: {elapsed:.2f}s")
        return result
    except Exception as e:
        elapsed = time.time() - t
        results.append(f"[ERR] {label}: {elapsed:.2f}s  ERROR={str(e)[:120]}")
        raise

try:
    results.append("STEP 1: Parse connection string -> URL")
    from app.utils.sqlserver_conn_parser import parse_sqlserver_connection_string
    parsed, url = timed("parse", lambda: parse_sqlserver_connection_string(CONN_STR))
    results.append(f"  URL: {url[:70]}...")
    results.append(f"  uses odbc_connect: {'odbc_connect' in url}")

    results.append("")
    results.append("STEP 2: Raw SQLAlchemy connect")
    from sqlalchemy import create_engine, text
    def raw_connect():
        e = create_engine(url, pool_pre_ping=True)
        with e.connect() as c:
            c.execute(text("SELECT 1"))
        e.dispose()
    timed("raw SQLAlchemy", raw_connect)

    results.append("")
    results.append("STEP 3: set_database_url (1 internal SELECT 1 test)")
    from app.db.session import set_database_url, reset_database_url, get_schema_summary
    timed("set_database_url", lambda: set_database_url(url))

    results.append("")
    results.append("STEP 4: get_schema_summary")
    schema = timed("get_schema_summary", get_schema_summary)
    results.append(f"  Tables: {schema.count('Table:')}")

    results.append("")
    results.append("STEP 5: reset_database_url (back to SQLite)")
    timed("reset_database_url", reset_database_url)

except Exception as e:
    results.append(f"FATAL: {e}")

# Write results to a plain ASCII file
with open("diag_results.txt", "w", encoding="ascii", errors="replace") as f:
    for line in results:
        f.write(line + "\n")

# Also print
for line in results:
    print(line)
