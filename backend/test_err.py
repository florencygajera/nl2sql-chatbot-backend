from fastapi.testclient import TestClient
from app.main import app
import traceback

client = TestClient(app)
try:
    response = client.post("/api/v1/chat", json={"message": "how many users?"})
    with open("err_trace.txt", "w") as f:
        f.write(str(response.status_code) + "\n" + response.text)
except Exception as e:
    with open("err_trace.txt", "w") as f:
        f.write(traceback.format_exc())
