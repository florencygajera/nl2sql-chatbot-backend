import requests, json, time

body = {
    "connection_string": (
        "Data Source=6gd6btjtnbtux3fhcs6psyufz1ec6gyh3i8hfmkpcmhwdtvwuozaxu8uoptlcsk.printftech.com,2408;"
        "Initial Catalog=DB_GramBook_v11.0;"
        "User ID=Demo;"
        "Password=sa@123;"
        "TrustServerCertificate=True;"
        "Integrated Security=False;"
    ),
    "db_type": "mssql",
    "test_connection": True,
}

print("Connecting via API...")
start = time.time()
r = requests.post("http://127.0.0.1:8000/api/v1/db/connect", json=body, timeout=30)
elapsed = time.time() - start
print(f"Status: {r.status_code} | took: {elapsed:.2f}s")

resp = r.json()
if r.status_code == 200:
    schema = resp.get("schema", "")
    tables = [l for l in schema.splitlines() if l.startswith("Table:")]
    sid = str(resp.get("db_session_id", ""))[:30]
    print(f"ok={resp.get('ok')} | session_id={sid}")
    print(f"Tables in schema: {len(tables)}")
    print("Sample tables:", tables[:5])
else:
    print("Error:", json.dumps(resp, indent=2)[:600])
