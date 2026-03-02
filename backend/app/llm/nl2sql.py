from __future__ import annotations

import re
from app.llm.client import LLMClient

_client = LLMClient()

# Known tables and columns from the database schema
KNOWN_TABLES: dict[str, list[str]] = {
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

TABLES_LIST = """- User_Master (Id, FirstName, LastName, Address, UserName, Password, Role, ContactNo, EmailId, AgencyName, IsActive)
- TaxRequest_Master (Id, UserId, InvoiceNo, InvoiceDate, CustomerName, Address, BasicAmount, TaxAmount, Status, VehicleNo)
- Receipt_Master (Id, TaxRequestId, ReceiptNo, ReceiptDate, Amount, PaymentMode, Status, ContactNo)
- Vehicle_Master (Id, UserId, InvoiceNo, CustomerName, ContactNo, BasicAmount, VehicleType, EngineNo, ChassisNo)
- VehicleType_Master (Id, VehicleType, FuelType)
- TaxRequest_Status (Id, TaxRequestId, Status, Remarks)
- TaxPay_Online (OrderId, TaxRequestId, Status, ContactNo, Amount)
- Login_Token (Id, UserId, UserName, JwtToken, IsRevoked)
- Otp_Master (Id, UserId, Otp, ContactNo, ExpiryTime)
- IdProof_Type (Id, UserId, IdProofType)
- User_VehicleType (Id, UserId, VehicleTypeId)"""


def _normalize_llm_sql(raw_sql: str) -> str:
    """Normalize LLM SQL so it is executable:
    - remove ```sql fences
    - remove surrounding quotes (if whole query is wrapped)
    - fix doubled identifier quotes: ""Table"" -> "Table"
    """
    if not raw_sql:
        return ""

    s = raw_sql.strip()

    # Remove code fences
    s = re.sub(r"^\s*```(?:sql)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```\s*$", "", s, flags=re.IGNORECASE)
    s = s.strip()

    # If the entire query is wrapped in a single pair of quotes
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()

    # Fix doubled quotes
    s = s.replace('""', '"')

    return s.strip()


def _quote_postgres_outside_literals(sql: str) -> str:
    """
    Quote ONLY known table/column identifiers for PostgreSQL, but NEVER touch:
    - string literals: '...'
    - already double-quoted identifiers: "..."
    This avoids breaking values like 'Car' and avoids ""Table"" problems.
    """
    if not sql:
        return ""

    # First fix doubled quotes globally
    sql = sql.replace('""', '"')

    # Split into parts: even indexes = outside '...'; odd indexes = inside '...'
    # This handles escaped quotes inside strings: '' (SQL standard)
    parts = re.split(r"('(?:''|[^'])*')", sql)

    def quote_identifiers(segment: str) -> str:
        # Do not mess with anything already in "double quotes"
        # So we split further by double-quoted chunks and only edit outside them.
        subparts = re.split(r'("(?:[^"]|"")*")', segment)

        for i in range(0, len(subparts), 2):  # only outside double-quotes
            s = subparts[i]

            # Quote table names
            for table in KNOWN_TABLES:
                pattern = r'\b' + re.escape(table) + r'\b'
                s = re.sub(pattern, f'"{table}"', s, flags=re.IGNORECASE)

            # Quote column names
            for cols in KNOWN_TABLES.values():
                for col in cols:
                    pattern = r'\b' + re.escape(col) + r'\b'
                    s = re.sub(pattern, f'"{col}"', s, flags=re.IGNORECASE)

            subparts[i] = s

        return "".join(subparts)

    # Apply quoting only to non-literal parts (outside single quotes)
    for i in range(0, len(parts), 2):
        parts[i] = quote_identifiers(parts[i])

    return "".join(parts)


def generate_sql(user_message: str, schema_hint: str = "") -> str:
    user_lower = (user_message or "").lower()

    # Detect dialect
    wants_postgres = any(k in user_lower for k in ["postgresql", "postgres", "postgre", "pg"])
    wants_mysql = "mysql" in user_lower
    wants_sqlite = "sqlite" in user_lower
    wants_generic_sql = ("sql" in user_lower) and not (wants_postgres or wants_mysql or wants_sqlite)

    if not (wants_postgres or wants_mysql or wants_sqlite or wants_generic_sql):
        return "Please specify which SQL dialect you want: PostgreSQL, MySQL, SQLite, or generic SQL."

    extra = schema_hint.strip()
    extra_block = f"\n\nExtra schema hint:\n{extra}\n" if extra else ""

    # --- Build prompt ---
    if wants_postgres:
        # IMPORTANT: because your real identifiers are mixed case,
        # Postgres needs "DoubleQuotes" always.
        prompt = f"""
You are a SQL generator.

Return ONLY ONE PostgreSQL SELECT query. No markdown. No explanations.
Do NOT wrap output in ```sql fences.

CRITICAL POSTGRES RULES:
- ALWAYS use double quotes for ALL table and column names exactly as in the schema.
  Example: SELECT "Id" FROM "User_Master";
- NEVER output doubled quotes like ""User_Master"".
- Do not invent tables/columns. Use only the schema below.

Tables:
{TABLES_LIST}{extra_block}
Question: {user_message}
""".strip()

    elif wants_mysql:
        prompt = f"""
You are a SQL generator.

Return ONLY ONE MySQL SELECT query. No markdown. No explanations.
Do NOT wrap output in ```sql fences.
Do not invent tables/columns. Use only the schema below.

Tables:
{TABLES_LIST}{extra_block}
Question: {user_message}
""".strip()

    elif wants_sqlite:
        prompt = f"""
You are a SQL generator.

Return ONLY ONE SQLite SELECT query. No markdown. No explanations.
Do NOT wrap output in ```sql fences.
Do not invent tables/columns. Use only the schema below.

Tables:
{TABLES_LIST}{extra_block}
Question: {user_message}
""".strip()

    else:
        prompt = f"""
You are a SQL generator.

Return ONLY ONE generic SELECT query. No markdown. No explanations.
Do NOT wrap output in ```sql fences.
Avoid dialect-specific quoting unless necessary.
Do not invent tables/columns. Use only the schema below.

Tables:
{TABLES_LIST}{extra_block}
Question: {user_message}
""".strip()

    raw = _client.generate(prompt).strip()
    raw = _normalize_llm_sql(raw)

    # ✅ BEST PRACTICE FOR YOUR SCHEMA:
    # Your tables/columns are MixedCase, so Postgres MUST use quotes.
    if wants_postgres:
        raw = _quote_postgres_outside_literals(raw)

    return raw