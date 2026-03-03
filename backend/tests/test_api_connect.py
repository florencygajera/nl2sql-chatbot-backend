import os

import pytest
import requests


pytestmark = pytest.mark.integration


def test_live_db_connect_endpoint() -> None:
    """Optional live integration test for /api/v1/db/connect.

    This test is skipped by default because it requires:
    - A running API server at http://127.0.0.1:8000
    - External network connectivity to the demo MSSQL host
    """
    if os.getenv("RUN_LIVE_API_TESTS", "").lower() not in {"1", "true", "yes"}:
        pytest.skip("Set RUN_LIVE_API_TESTS=1 to run live API integration tests")

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

    response = requests.post(
        "http://127.0.0.1:8000/api/v1/db/connect",
        json=body,
        timeout=30,
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert data.get("ok") is True
    assert isinstance(data.get("schema", ""), str)
