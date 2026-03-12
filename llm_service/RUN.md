# NL2SQL LLM Service - Setup and Usage Guide

## Windows Setup Instructions

### Prerequisites

1. **Python 3.10 or higher** - Download from python.org
2. **CUDA (optional)** - For GPU acceleration, requires CUDA 11.8+ and cuDNN 8.6+

### Step 1: Create Virtual Environment

```powershell
# Navigate to the project directory
cd d:\nl2sql-chatbot-backend\llm_service

# Create virtual environment
python -m venv venv

# Activate the virtual environment
.\venv\Scripts\activate
```

### Step 2: Install Dependencies

```powershell
# Upgrade pip
python -m pip install --upgrade pip

# Install PyTorch (CPU version - recommended for initial setup)
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu

# Or for GPU (if CUDA is available)
# pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

# Install the required packages
pip install -r requirements.txt
```

### Step 3: Generate Training Data

```powershell
# Generate synthetic training data from sample schema
python data_prep.py --sample --output ./data/train_dataset.json

# Or from your own schema_catalog.json
python data_prep.py --schema ../backend/data/schema_catalog.json --output ./data/train_dataset.json
```

### Step 4: Train the Model

```powershell
# Train with LoRA (recommended for local fine-tuning)
python train_lora.py --data ./data/train_dataset.json --output ./output --epochs 3

# Or with 8-bit quantization to reduce memory usage
python train_lora.py --data ./data/train_dataset.json --output ./output --use-8bit --epochs 3

# Or with 4-bit quantization for even lower memory
python train_lora.py --data ./data/train_dataset.json --output ./output --use-4bit --epochs 3
```

### Step 5: Run the Inference Server

```powershell
# Start the inference server on CPU
python serve.py --model ./output/final --port 8000

# Or on GPU
python serve.py --model ./output/final --port 8000 --device cuda
```

### Step 6: Evaluate the Model

```powershell
# Run evaluation with sample queries
python eval.py --url http://localhost:8000 --sample

# Or with your test data
python eval.py --url http://localhost:8000 --test-data ./data/test_dataset.json
```

## Alternative: Use Pre-trained Model

If you don't want to train, you can use the base model directly:

```powershell
# Use Qwen2.5-7B-Instruct directly (requires ~14GB RAM)
python serve.py --model Qwen/Qwen2.5-7B-Instruct --port 8000

# Or the smaller 3B model (requires ~6GB RAM)
python serve.py --model Qwen/Qwen2.5-3B-Instruct --port 8000
```

## API Usage

### Generate SQL Query

```powershell
# Using curl
$body = @{
    dialect = "postgres"
    question = "How many users are there?"
    schema_summary = "Table: users - id, username, email"
    retrieved_schema_snippet = "Table: users - id (INTEGER), username (VARCHAR)"
    max_tokens = 512
    temperature = 0.2
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8000/generate" -Method Post -Body $body -ContentType "application/json"
```

### Health Check

```powershell
Invoke-RestMethod -Uri "http://localhost:8000/health"
```

## Configuration Options

### Training Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--model` | Qwen/Qwen2.5-7B-Instruct | Base model to fine-tune |
| `--data` | required | Training data JSON file |
| `--output` | ./output | Output directory |
| `--epochs` | 3 | Number of training epochs |
| `--batch-size` | 2 | Per-device batch size |
| `--learning-rate` | 2e-4 | Learning rate |
| `--use-8bit` | false | Use 8-bit quantization |
| `--use-4bit` | false | Use 4-bit quantization |
| `--merge` | false | Merge LoRA weights after training |

### Server Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--model` | ./output/final | Model path |
| `--adapter` | None | LoRA adapter path |
| `--port` | 8000 | Server port |
| `--host` | 0.0.0.0 | Server host |
| `--device` | auto | Device (auto/cpu/cuda) |
| `--no-load` | false | Don't load model on startup |

### Generation Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dialect` | postgres | SQL dialect (postgres/mysql/mssql/sqlite/oracle) |
| `question` | required | Natural language question |
| `schema_summary` | "" | Full schema summary |
| `retrieved_schema_snippet` | "" | TF-IDF retrieved relevant schema |
| `max_tokens` | 512 | Maximum tokens to generate |
| `temperature` | 0.2 | Sampling temperature (0 = deterministic) |
| `top_p` | 0.9 | Nucleus sampling parameter |
| `top_k` | 50 | Top-k sampling parameter |

## Troubleshooting

### Out of Memory Errors

1. Use quantization: `--use-8bit` or `--use-4bit`
2. Reduce batch size: `--batch-size 1`
3. Use smaller model: `Qwen/Qwen2.5-3B-Instruct`
4. Use gradient checkpointing (enabled by default)

### Model Not Loading

1. Check internet connection (for initial download)
2. Verify model path exists
3. Check disk space
4. Try with `--no-load` flag and load via API

### Server Not Starting

1. Check port is not in use
2. Verify firewall settings
3. Check logs for errors
4. Try different port: `--port 8001`

## Production Deployment

For production deployment on Windows:

1. Use Windows Service or NSSM for background process
2. Configure logging to file
3. Set up monitoring and health checks
4. Use GPU for better performance
5. Consider using ONNX runtime for faster inference

## File Structure

```
llm_service/
├── requirements.txt    # Python dependencies
├── prompts.py         # Prompt engineering system
├── data_prep.py       # Data preparation and synthetic generation
├── train_lora.py     # LoRA training pipeline
├── serve.py          # FastAPI inference server
├── eval.py           # Evaluation pipeline
├── RUN.md            # This file
├── data/             # Training data (generated)
└── output/           # Model output (generated)