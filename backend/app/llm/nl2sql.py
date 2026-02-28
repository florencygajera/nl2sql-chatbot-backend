from __future__ import annotations

from app.llm.client import LLMClient


_client = LLMClient()

# Known tables and columns from the database schema
KNOWN_TABLES = {
    "User_Master": ["Id", "FirstName", "LastName", "Address", "UserName", "Password", "Role", "ContactNo", "EmailId", "AgencyName", "IsActive"],
    "TaxRequest_Master": ["Id", "UserId", "InvoiceNo", "InvoiceDate", "CustomerName", "Address", "BasicAmount", "TaxAmount", "Status", "VehicleNo"],
    "Receipt_Master": ["Id", "TaxRequestId", "ReceiptNo", "ReceiptDate", "Amount", "PaymentMode", "Status", "ContactNo"],
    "Vehicle_Master": ["Id", "UserId", "InvoiceNo", "CustomerName", "ContactNo", "BasicAmount", "VehicleType", "EngineNo", "ChassisNo"],
    "VehicleType_Master": ["Id", "VehicleType", "FuelType"],
    "TaxRequest_Status": ["Id", "TaxRequestId", "Status", "Remarks"],
    "TaxPay_Online": ["OrderId", "TaxRequestId", "Status", "ContactNo", "Amount"],
    "Login_Token": ["Id", "UserId", "UserName", "JwtToken", "IsRevoked"],
    "Otp_Master": ["Id", "UserId", "Otp", "ContactNo", "ExpiryTime"],
    "IdProof_Type": ["Id", "UserId", "IdProofType"],
    "User_VehicleType": ["Id", "UserId", "VehicleTypeId"],
}


def _quote_postgres(text: str) -> str:
    """Add double quotes to table and column names for PostgreSQL."""
    import re
    result = text
    # Quote table names
    for table in KNOWN_TABLES:
        result = re.sub(r'\b' + table + r'\b', f'"{table}"', result, flags=re.IGNORECASE)
    # Quote column names
    for table, cols in KNOWN_TABLES.items():
        for col in cols:
            result = re.sub(r'\b' + col + r'\b', f'"{col}"', result, flags=re.IGNORECASE)
    return result


def generate_sql(user_message: str, schema_hint: str) -> str:
    user_lower = user_message.lower()
    
    # Check which database the user explicitly requested
    wants_postgres = any(keyword in user_lower for keyword in ['postgresql', 'postgres', 'postgre', 'pg'])
    wants_mysql = 'mysql' in user_lower
    wants_sqlite = 'sqlite' in user_lower
    wants_generic_sql = 'sql' in user_lower and not wants_postgres
    
    # If no explicit dialect specified, return a clarification message
    if not (wants_postgres or wants_mysql or wants_sqlite or wants_generic_sql):
        return "Please specify which SQL dialect you want: PostgreSQL, MySQL, SQLite, or generic SQL."
    
    # Simplified schema
    tables_list = """- User_Master (Id, FirstName, LastName, Address, UserName, Password, Role, ContactNo, EmailId)
- TaxRequest_Master (Id, UserId, InvoiceNo, InvoiceDate, CustomerName, Address, BasicAmount, TaxAmount, Status)
- Receipt_Master (Id, TaxRequestId, ReceiptNo, ReceiptDate, Amount, PaymentMode, Status)
- Vehicle_Master (Id, UserId, InvoiceNo, CustomerName, ContactNo, BasicAmount, VehicleType)
- VehicleType_Master (Id, VehicleType, FuelType)
- TaxRequest_Status (Id, TaxRequestId, Status, Remarks)
- TaxPay_Online (OrderId, TaxRequestId, Status, ContactNo, Amount)
- Login_Token (Id, UserId, UserName, JwtToken, IsRevoked)
- Otp_Master (Id, UserId, Otp, ContactNo, ExpiryTime)
- IdProof_Type (Id, UserId, IdProofType)
- User_VehicleType (Id, UserId, VehicleTypeId)"""

    if wants_postgres:
        prompt = f"""Generate a PostgreSQL SQL query. 
Tables:
{tables_list}

Question: {user_message}

Output ONLY the PostgreSQL query with double quotes around table and column names.
""".strip()
    elif wants_mysql:
        prompt = f"""Generate a MySQL SQL query.
Tables:
{tables_list}

Question: {user_message}

Output ONLY the MySQL query.
""".strip()
    elif wants_sqlite:
        prompt = f"""Generate a SQLite SQL query.
Tables:
{tables_list}

Question: {user_message}

Output ONLY the SQLite query.
""".strip()
    else:
        prompt = f"""Generate a generic SQL query.
Tables:
{tables_list}

Question: {user_message}

Output ONLY the generic SQL query without any dialect-specific quoting.
""".strip()
    
    raw = _client.generate(prompt).strip()
    
    import re
    # If the LLM wraps the query in markdown
    match = re.search(r"```(?:sql)?\s*(.*?)\s*```", raw, re.DOTALL | re.IGNORECASE)
    if match:
        raw = match.group(1).strip()
    
    # For PostgreSQL, ensure all table and column names are quoted
    if wants_postgres:
        raw = _quote_postgres(raw)
        
    return raw