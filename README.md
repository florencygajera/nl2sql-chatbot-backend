# NL2SQL Chatbot Backend - Complete Local System

A fully local Natural Language to SQL system that converts natural language questions into SQL queries using a local LLM model. No external APIs, no internet required after initial model download.

## Architecture

```
Frontend (querymind.html)
    ↓
Backend API (FastAPI)
    ↓
Database Connector → Schema Introspection
    ↓
Schema Catalog Builder
    ↓
Schema Retrieval Engine (TF-IDF)
    ↓
Local NL2SQL LLM Service (Qwen2.5-7B-Instruct with LoRA)
    ↓
SQL Validation Layer (sqlglot)
    ↓
SQL Safety Firewall
    ↓
Row Limit Injection
    ↓
Query Execution
    ↓
JSON Response
```

## Project Structure

```
nl2sql-chatbot-backend/
├── llm_service/              # Local NL2SQL LLM training and inference
│   ├── requirements.txt       # Python dependencies
│   ├── prompts.py            # Dialect-aware prompt templates
│   ├── data_prep.py          # Synthetic dataset generation
│   ├── train_lora.py         # LoRA fine-tuning pipeline
│   ├── serve.py              # FastAPI inference server
│   ├── eval.py               # Evaluation pipeline
│   ├── RUN.md                # Windows setup guide
│   └── data/                 # Training data (generated)
│   └── output/               # Model output (generated)
│
├── backend/                   # FastAPI backend
│   ├── app/
│   │   ├── api/
│   │   │   ├── routes.py     # Original chat endpoints
│   │   │   └── nl2sql_routes.py  # New NL2SQL endpoints
│   │   ├── db/
│   │   │   ├── session.py    # Database connections
│   │   │   ├── schema_tfidf.py   # TF-IDF retrieval
│   │   │   ├── sql_validator.py  # SQL validation
│   │   │   └── schema_cache.py
│   │   ├── llm/
│   │   │   ├── local_client.py   # Local LLM client
│   │   │   └── ...
│   │   ├── security/
│   │   │   └── sql_guard.py  # SQL safety checks
│   │   └── services/
│   │       ├── nl2sql_orchestrator.py  # Main orchestrator
│   │       └── ...
│   ├── requirements.txt
│   └── ...
│
└── frontend/
    └── querymind.html        # User interface
```

## Quick Start (Windows)

### 1. Install Backend Dependencies

```powershell
cd d:\nl2sql-chatbot-backend\backend
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Install LLM Service Dependencies

```powershell
cd d:\nl2sql-chatbot-backend\llm_service
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Generate Training Data (Optional - Skip to use base model)

```powershell
cd d:\nl2sql-chatbot-backend\llm_service
python data_prep.py --sample --output ./data/train_dataset.json
```

### 4. Train the Model (Optional - Skip to use base model)

```powershell
python train_lora.py --data ./data/train_dataset.json --output ./output --epochs 3
```

### 5. Start the LLM Inference Server

```powershell
# Using base model directly (no training required)
python serve.py --model Qwen/Qwen2.5-7B-Instruct --port 8000

# Or using trained model
python serve.py --model ./output/final --port 8000
```

### 6. Start the Backend API

```powershell
cd d:\nl2sql-chatbot-backend\backend
uvicorn app.main:app --reload --port 8001
```

### 7. Open the Frontend

Open `frontend/querymind.html` in a browser.

## New API Endpoints

The new NL2SQL endpoints are available at:

- **POST** `/api/v1/nl2sql/generate` - Generate and execute SQL
- **POST** `/api/v1/nl2sql/generate-sql-only` - Generate SQL only
- **GET** `/api/v1/nl2sql/schema-info` - Get current schema info
- **GET** `/api/v1/nl2sql/llm-health` - Check LLM service health

### Example Request

```json
{
  "question": "How many users are there?",
  "return_sql_only": false,
  "max_rows": 1000
}
```

### Example Response

```json
{
  "success": true,
  "question": "How many users are there?",
  "sql_query": "SELECT COUNT(*) FROM users LIMIT 1000",
  "dialect": "postgres",
  "data": [{"count": 150}],
  "column_names": ["count"],
  "timing_ms": {
    "retrieval_ms": 12.5,
    "generation_ms": 450.2,
    "execution_ms": 25.8,
    "total_ms": 488.5
  }
}
```

## Features

### ✅ Fully Local
- No OpenAI, no Ollama, no external APIs
- All processing happens on your machine
- Works completely offline after model download

### ✅ Multiple Database Dialects
- PostgreSQL
- MySQL
- MSSQL
- SQLite
- Oracle

### ✅ SQL Safety
- Blocks dangerous keywords (DROP, DELETE, UPDATE, etc.)
- Single SELECT statement only
- Row limit enforcement (max 1000 rows)
- Comment injection protection

### ✅ Schema Retrieval
- TF-IDF based relevance matching
- Synonym expansion
- Keyword boost for ID/Date/Status/Amount columns

### ✅ SQL Validation
- Syntax validation using sqlglot
- Table/column existence checking
- Query type validation

## Configuration

### Environment Variables (backend/.env)

```env
# Database
DATABASE_URL=postgresql://user:pass@localhost/dbname

# LLM Service (Local NL2SQL)
LLM_SERVICE_URL=http://localhost:8000

# Query settings
DEFAULT_ROW_LIMIT=50
MAX_ROW_LIMIT=500
```

## Troubleshooting

### LLM Service Not Available

If you see "LLM service not available", the system will use a fallback SQL generator that provides simple template-based SQL. To fix:

1. Ensure the inference server is running: `python serve.py --model Qwen/Qwen2.5-7B-Instruct`
2. Check the URL in config or environment variable

### Out of Memory

For training with limited memory:
```powershell
python train_lora.py --data ./data/train_dataset.json --use-4bit --batch-size 1
```

For inference with limited memory:
```powershell
# Use smaller model
python serve.py --model Qwen/Qwen2.5-3B-Instruct --port 8000
```

### Database Connection Issues

1. Check DATABASE_URL in .env
2. Ensure database server is running
3. Test connection with: `python -c "from app.db.session import ping_database; print(ping_database())"`

## License

MIT