# 🧠 NL2SQL Chatbot Backend

A **production-ready Natural Language to SQL chatbot backend** built with FastAPI, SQLAlchemy 2.0, PostgreSQL, and an OpenAI-compatible LLM.

Users type plain-English questions about an employee database and receive safe, validated SQL queries and/or structured results.

---

## 📂 Project Structure

```
backend/
├── app/
│   ├── main.py                  # FastAPI app factory & entry point
│   ├── core/
│   │   └── config.py            # Pydantic-settings configuration
│   ├── db/
│   │   └── session.py           # Engine, session factory, schema introspection
│   ├── security/
│   │   └── sql_guard.py         # SQL validation & injection prevention
│   ├── services/
│   │   ├── chat_service.py      # Orchestration: LLM → guard → execute → format
│   │   └── query_executor.py    # Safe parameterised query execution
│   ├── llm/
│   │   ├── client.py            # HTTP client for OpenAI-compatible API
│   │   └── prompt_builder.py    # System prompt + mode detection
│   └── api/
│       ├── routes.py            # /health and /chat endpoints
│       └── schemas.py           # Pydantic request/response models
├── tests/
│   ├── test_sql_guard.py        # Unit tests for SQL security layer
│   └── test_chat_endpoint.py    # Integration tests with mocked LLM
├── scripts/
│   └── seed_db.sql              # Sample employee DB schema + seed data
├── requirements.txt
├── .env.example
└── README.md
```

---

## ⚙️ Setup Instructions

### 1. Prerequisites

- Python 3.11+
- PostgreSQL 14+
- An OpenAI API key (or any OpenAI-compatible provider)

### 2. Clone & Install

```bash
git clone <repo-url>
cd backend

python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

### 3. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` with your values:

```dotenv
DATABASE_URL=postgresql+psycopg2://postgres:yourpassword@localhost:5432/employees
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o-mini
```

### 4. Set Up the Database

```bash
# Create the database
createdb -U postgres employees

# Load the sample schema and seed data
psql -U postgres -d employees -f scripts/seed_db.sql
```

### 5. Run the Server

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Open the interactive docs at: **http://localhost:8000/docs**

---

## 🚀 How to Run Tests

```bash
pytest tests/ -v
```

---

## 📡 API Endpoints

### `GET /api/v1/health`

Returns server + database status.

```json
{
  "status": "ok",
  "database": "connected",
  "version": "1.0.0"
}
```

---

### `POST /api/v1/chat`

**Request body:**
```json
{ "message": "string (1–2000 chars)" }
```

**Response — DB query:**
```json
{
  "type": "DB",
  "mode": "QUERY_AND_ANSWER",
  "sql": "SELECT SUM(e.salary) AS total_salary FROM employees e",
  "params": {},
  "explanation": "Calculates total salary of all employees.",
  "result": {
    "columns": ["total_salary"],
    "rows": [[4250000.00]],
    "row_count": 1
  },
  "answer_text": "Calculates total salary of all employees. Result: total_salary = 4250000.0"
}
```

**Response — Chat:**
```json
{
  "type": "CHAT",
  "answer": "I'm here to help you query the employee database!"
}
```

**Response — Clarification needed:**
```json
{
  "type": "CLARIFY",
  "question": "Which department are you asking about?",
  "missing": ["department name"]
}
```

---

## 🔀 Mode Detection

| User says | Mode |
|-----------|------|
| "give me the **sql** query" | `QUERY_ONLY` — returns SQL only, no execution |
| "**query** for department salaries" | `QUERY_ONLY` |
| "**command only** for top earners" | `QUERY_ONLY` |
| "**only answer**, no sql" | `ANSWER_ONLY` — executes and returns results, no SQL |
| Anything else | `QUERY_AND_ANSWER` — returns both SQL and results |

---

## 🔒 Security Architecture

All LLM-generated SQL passes through `sql_guard.py` before execution:

| Check | Details |
|-------|---------|
| **SELECT-only** | Any statement not starting with SELECT is rejected |
| **Forbidden keywords** | INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE, GRANT, REVOKE, … |
| **Multiple statements** | Semicolons outside string literals are blocked |
| **Comment injection** | `--`, `/*`, `*/`, `#` patterns are blocked |
| **Auto LIMIT** | Non-aggregate queries get `LIMIT 50` injected automatically |
| **Hard ceiling** | Results are truncated to `MAX_ROW_LIMIT=500` regardless |
| **Parameterised execution** | Only `:param_name` style — no raw user values in SQL |
| **Read-only transaction** | `SET TRANSACTION READ ONLY` attempted on every query |

---

## 💬 Example curl Requests

### Health check
```bash
curl http://localhost:8000/api/v1/health
```

### Total salary (QUERY_AND_ANSWER)
```bash
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "Total salary paid to all employees"}'
```

### Department-wise salary totals
```bash
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "Department-wise total salary"}'
```

### Top 5 earners in IT (SQL only)
```bash
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "Give me only the SQL query for top 5 highest paid employees in IT"}'
```

### Employees hired after 2020 (answer only)
```bash
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "List employees hired after 2020 — only answer please"}'
```

### General chat
```bash
curl -X POST http://localhost:8000/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "Hello, what can you help me with?"}'
```

---

## 🗣️ 10 Example Natural Language Queries

| # | User Query | Expected Behaviour |
|---|-----------|-------------------|
| 1 | `"Total salary paid to all employees"` | `DB / QUERY_AND_ANSWER` — `SELECT SUM(salary)…`, returns single aggregate value |
| 2 | `"Department-wise total salary"` | `DB / QUERY_AND_ANSWER` — `SELECT d.name, SUM(e.salary) … GROUP BY d.name` |
| 3 | `"Top 5 highest paid employees in IT"` | `DB / QUERY_AND_ANSWER` — `WHERE d.name = 'IT' ORDER BY salary DESC LIMIT 5` |
| 4 | `"List employees hired after 2020"` | `DB / QUERY_AND_ANSWER` — `WHERE hire_date > '2020-01-01'` + auto LIMIT 50 |
| 5 | `"Give me only the SQL query for department salary totals"` | `DB / QUERY_ONLY` — returns SQL, does **not** execute |
| 6 | `"Average salary by job title, only answer"` | `DB / ANSWER_ONLY` — executes and returns results, **no SQL** in response |
| 7 | `"How many employees are in each department?"` | `DB / QUERY_AND_ANSWER` — `COUNT(*) GROUP BY department` |
| 8 | `"Who is the highest paid employee?"` | `DB / QUERY_AND_ANSWER` — `ORDER BY salary DESC LIMIT 1` |
| 9 | `"Employees with salary above 100000 in Engineering"` | `DB / QUERY_AND_ANSWER` — filtered by department + salary |
| 10 | `"Hello, what can you help me with?"` | `CHAT` — general conversational reply, no SQL |
| 11 | `"Show me employees from the department"` | `CLARIFY` — asks which department |
| 12 | `"Performance review scores for all employees"` | `DB / QUERY_AND_ANSWER` — JOINs `performance_reviews` + `employees` |

---

## 🔧 Using an Alternative LLM Provider

### Groq (fast, cheap)
```dotenv
OPENAI_BASE_URL=https://api.groq.com/openai/v1
OPENAI_MODEL=llama-3.3-70b-versatile
OPENAI_API_KEY=gsk_...
```

### Ollama (local, free)
```dotenv
OPENAI_BASE_URL=http://localhost:11434/v1
OPENAI_MODEL=llama3.2
OPENAI_API_KEY=ollama
```

---

## 🏗️ Architecture Overview

```
User Request
     │
     ▼
┌─────────────────────┐
│   FastAPI /chat     │  ← validates input (Pydantic)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   chat_service.py   │  ← orchestrates the full pipeline
└──────┬──────┬───────┘
       │      │
       │      ▼
       │  ┌──────────────────┐
       │  │  prompt_builder  │  ← injects live DB schema into system prompt
       │  └────────┬─────────┘
       │           │
       │           ▼
       │  ┌──────────────────┐
       │  │   llm/client     │  ← calls OpenAI-compatible API, parses JSON
       │  └────────┬─────────┘
       │           │
       │           ▼
       │  ┌──────────────────┐
       │  │   sql_guard      │  ← validates & sanitizes SQL (blocks injections)
       │  └────────┬─────────┘
       │           │
       │           ▼
       │  ┌──────────────────┐
       │  │ query_executor   │  ← parameterised execution, read-only tx
       │  └────────┬─────────┘
       │           │
       └───────────┘
           │
           ▼
    Structured JSON Response
    (DB | CHAT | CLARIFY)
```

---

## 📦 Dependencies

| Package | Purpose |
|---------|---------|
| `fastapi` | Web framework & API routing |
| `uvicorn` | ASGI server |
| `sqlalchemy 2.0` | ORM, engine, schema inspection |
| `psycopg2-binary` | PostgreSQL driver |
| `pydantic` + `pydantic-settings` | Validation & env config |
| `httpx` | Async HTTP client for LLM API |
| `pytest` + `pytest-asyncio` | Testing |
