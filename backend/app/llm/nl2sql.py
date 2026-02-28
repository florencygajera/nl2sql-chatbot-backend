from __future__ import annotations

from app.llm.client import LLMClient


_client = LLMClient()

# Known tables from the database schema
KNOWN_TABLES = [
    "User_Master",
    "TaxRequest_Master", 
    "Receipt_Master",
    "Vehicle_Master",
    "VehicleType_Master",
    "User_VehicleType",
    "TaxRequest_Status",
    "TaxPay_Online",
    "Login_Token",
    "Otp_Master",
    "IdProof_Type",
]


def generate_sql(user_message: str, schema_hint: str) -> str:
    # Detect if user wants PostgreSQL specifically
    user_lower = user_message.lower()
    is_postgres = any(keyword in user_lower for keyword in [
        'postgresql', 'postgres', 'postgre', 'pg'
    ])
    
    # Simplified schema - only show table and column names
    tables_list = """- "User_Master" (Id, FirstName, LastName, Address, UserName, Password, Role, ContactNo, EmailId)
- "TaxRequest_Master" (Id, UserId, InvoiceNo, InvoiceDate, CustomerName, Address, BasicAmount, TaxAmount, Status)
- "Receipt_Master" (Id, TaxRequestId, ReceiptNo, ReceiptDate, Amount, PaymentMode, Status)
- "Vehicle_Master" (Id, UserId, InvoiceNo, CustomerName, ContactNo, BasicAmount, VehicleType)
- "VehicleType_Master" (Id, VehicleType, FuelType)
- "TaxRequest_Status" (Id, TaxRequestId, Status, Remarks)
- "TaxPay_Online" (OrderId, TaxRequestId, Status, ContactNo, Amount)
- "Login_Token" (Id, UserId, UserName, JwtToken, IsRevoked)
- "Otp_Master" (Id, UserId, Otp, ContactNo, ExpiryTime)
- "IdProof_Type" (Id, UserId, IdProofType)
- "User_VehicleType" (Id, UserId, VehicleTypeId)"""

    if is_postgres:
        prompt = f"""Generate a PostgreSQL SQL query. 
Tables:
{tables_list}

Question: {user_message}

Output ONLY the SQL query with PostgreSQL syntax - use double quotes for table and column names.
""".strip()
    else:
        prompt = f"""Generate a SQL query. 
Tables:
{tables_list}

Question: {user_message}

Output ONLY the SQL query with standard SQL syntax - no double quotes needed.
""".strip()
    
    raw = _client.generate(prompt).strip()
    
    import re
    # If the LLM wraps the query in markdown
    match = re.search(r"```(?:sql)?\s*(.*?)\s*```", raw, re.DOTALL | re.IGNORECASE)
    if match:
        raw = match.group(1).strip()
    
    # Post-process: if PostgreSQL requested, ensure all known table names are quoted
    if is_postgres:
        for table in KNOWN_TABLES:
            raw = re.sub(r'\b' + table + r'\b', f'"{table}"', raw, flags=re.IGNORECASE)
        
    return raw