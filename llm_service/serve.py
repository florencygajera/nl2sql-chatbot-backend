"""
NL2SQL Local Inference Server
FastAPI server for running the trained NL2SQL model locally.
"""

import os
import sys
import json
import argparse
import logging
from pathlib import Path
from typing import Optional, Dict, List, Any, Union
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor

import torch
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from transformers import AutoModelForCausalLM, AutoTokenizer, GenerationConfig
from peft import PeftModel
import uvicorn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============== Data Models ==============

class GenerationRequest(BaseModel):
    """Request model for generation endpoint."""
    dialect: str = Field(default="postgres", description="SQL dialect")
    question: str = Field(..., description="Natural language question")
    schema_summary: str = Field(default="", description="Full schema summary")
    retrieved_schema_snippet: str = Field(default="", description="TF-IDF retrieved schema")
    max_tokens: int = Field(default=512, ge=1, le=2048)
    temperature: float = Field(default=0.2, ge=0.0, le=2.0)
    top_p: float = Field(default=0.9, ge=0.0, le=1.0)
    top_k: int = Field(default=50, ge=0)
    do_sample: bool = Field(default=True)
    num_beams: int = Field(default=1, ge=1)


class GenerationResponse(BaseModel):
    """Response model for generation endpoint."""
    raw_text: str
    parsed: Optional[Dict[str, Any]] = None
    errors: List[str] = []
    model_info: Dict[str, str] = {}
    timing: Dict[str, float] = {}


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    model_loaded: bool
    model_name: str
    device: str
    memory_usage: Dict[str, float] = {}


class ModelInfo(BaseModel):
    """Model information."""
    base_model: str
    adapter_path: Optional[str]
    is_loaded: bool
    device: str


# ============== Global State ==============

app = FastAPI(
    title="NL2SQL Local Inference Server",
    description="Local inference server for NL2SQL model",
    version="1.0.0"
)

# Global model and tokenizer
model = None
tokenizer = None
model_info: Dict[str, Any] = {
    "base_model": "",
    "adapter_path": None,
    "is_loaded": False,
    "device": "cpu"
}

# Thread pool for inference
inference_executor = ThreadPoolExecutor(max_workers=2)


# ============== Prompt Engineering ==============

SYSTEM_PROMPT = """You are a dialect-aware NL2SQL generator. Output ONLY valid JSON."""

JSON_REPAIR_PROMPT = """The following text was supposed to be valid JSON but isn't. Fix it and return ONLY valid JSON. No explanations, no markdown:

"""


def build_prompt(
    question: str,
    schema_summary: str,
    retrieved_schema_snippet: str,
    dialect: str
) -> str:
    """Build the prompt for the model."""
    prompt = f"""Schema (Full):
{schema_summary}

Relevant Schema (TF-IDF Retrieved):
{retrieved_schema_snippet}

Question: {question}

Target Dialect: {dialect}

Output only valid JSON with the SQL query. Use these keys:
- sql_query: The generated SQL
- dialect: The target dialect  
- confidence_score: 0.0-1.0
- referenced_tables: List of tables used
- needs_clarification: boolean
- clarification_question: Question if clarification needed

JSON:"""
    
    return prompt


def parse_json_response(response_text: str) -> Dict[str, Any]:
    """Parse model response into structured JSON with repair logic."""
    # First attempt: direct parse
    try:
        result = json.loads(response_text.strip())
        return {
            "parsed": result,
            "errors": []
        }
    except json.JSONDecodeError:
        pass
    
    # Second attempt: extract JSON from text
    import re
    patterns = [
        r'\{[^{}]*\}',  # Simple single-level JSON
        r'\{(?:[^{}]|\{(?:[^{}]|{[^{}]*})*\})*\}',  # Nested
    ]
    
    for pattern in patterns:
        match = re.search(pattern, response_text, re.DOTALL)
        if match:
            try:
                result = json.loads(match.group())
                return {
                    "parsed": result,
                    "errors": ["Extracted JSON from text"]
                }
            except json.JSONDecodeError:
                continue
    
    # Third attempt: clean markdown and retry
    try:
        cleaned = response_text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            if lines[-1].strip() == "```":
                cleaned = "\n".join(lines[1:-1])
            else:
                cleaned = "\n".join(lines[1:])
        result = json.loads(cleaned)
        return {
            "parsed": result,
            "errors": ["Cleaned markdown"]
        }
    except json.JSONDecodeError:
        pass
    
    # Fourth attempt: JSON repair via second prompt (placeholder)
    # In practice, would call model again with repair prompt
    
    return {
        "parsed": None,
        "errors": ["Failed to parse response as JSON", f"Response: {response_text[:200]}"]
    }


# ============== Model Loading ==============

def load_model(
    model_path: str,
    adapter_path: Optional[str] = None,
    device: str = "auto",
    use_flash_attention: bool = False
) -> None:
    """Load the model and tokenizer."""
    global model, tokenizer, model_info
    
    logger.info(f"Loading model from {model_path}")
    
    # Load tokenizer
    tokenizer = AutoTokenizer.from_pretrained(
        model_path,
        trust_remote_code=True,
        padding_side="right"
    )
    
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
    
    # Determine device
    if device == "auto":
        device = "cuda" if torch.cuda.is_available() else "cpu"
    
    # Load model
    dtype = torch.float16 if device == "cuda" else torch.float32
    
    model = AutoModelForCausalLM.from_pretrained(
        model_path,
        trust_remote_code=True,
        torch_dtype=dtype,
        device_map=device,
        use_flash_attention_2=use_flash_attention if device == "cuda" else False
    )
    
    # Load adapter if provided
    if adapter_path and os.path.exists(adapter_path):
        logger.info(f"Loading adapter from {adapter_path}")
        model = PeftModel.from_pretrained(model, adapter_path)
    
    # Set model to evaluation mode
    model.eval()
    
    # Update model info
    model_info = {
        "base_model": model_path,
        "adapter_path": adapter_path,
        "is_loaded": True,
        "device": device
    }
    
    logger.info(f"Model loaded successfully on {device}")


# ============== Inference ==============

def generate_sql(
    question: str,
    schema_summary: str,
    retrieved_schema_snippet: str,
    dialect: str,
    max_tokens: int = 512,
    temperature: float = 0.2,
    top_p: float = 0.9,
    top_k: int = 50,
    do_sample: bool = True,
    num_beams: int = 1
) -> GenerationResponse:
    """Generate SQL from natural language question."""
    import time
    
    if model is None or tokenizer is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    start_time = time.time()
    
    # Build prompt
    prompt = build_prompt(
        question=question,
        schema_summary=schema_summary,
        retrieved_schema_snippet=retrieved_schema_snippet,
        dialect=dialect
    )
    
    # Format messages for chat template
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": prompt}
    ]
    
    # Apply chat template
    text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    
    # Tokenize
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=2048)
    
    # Move to device
    if torch.cuda.is_available():
        inputs = {k: v.cuda() for k, v in inputs.items()}
    
    # Generation config
    gen_config = GenerationConfig(
        max_new_tokens=max_tokens,
        temperature=temperature,
        top_p=top_p,
        top_k=top_k,
        do_sample=do_sample,
        num_beams=num_beams,
        pad_token_id=tokenizer.pad_token_id,
        eos_token_id=tokenizer.eos_token_id,
        return_dict_in_generate=True,
        output_scores=False
    )
    
    # Generate
    with torch.no_grad():
        outputs = model.generate(**inputs, **gen_config)
    
    # Decode
    generated_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
    
    # Extract assistant response
    assistant_response = generated_text[len(text):].strip()
    
    # Parse JSON
    parse_result = parse_json_response(assistant_response)
    
    timing = {
        "total_seconds": time.time() - start_time
    }
    
    return GenerationResponse(
        raw_text=assistant_response,
        parsed=parse_result["parsed"],
        errors=parse_result["errors"],
        model_info=model_info,
        timing=timing
    )


# ============== API Endpoints ==============

@app.get("/", tags=["Root"])
async def root():
    """Root endpoint."""
    return {
        "service": "NL2SQL Local Inference Server",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health():
    """Health check endpoint."""
    memory_info = {}
    if torch.cuda.is_available():
        memory_info = {
            "allocated_gb": torch.cuda.memory_allocated() / 1e9,
            "reserved_gb": torch.cuda.memory_reserved() / 1e9
        }
    
    return HealthResponse(
        status="healthy" if model_info["is_loaded"] else "model_not_loaded",
        model_loaded=model_info["is_loaded"],
        model_name=model_info["base_model"],
        device=model_info["device"],
        memory_usage=memory_info
    )


@app.get("/model-info", response_model=ModelInfo, tags=["Model"])
async def get_model_info():
    """Get model information."""
    if not model_info["is_loaded"]:
        raise HTTPException(status_code=503, detail="Model not loaded")
    
    return ModelInfo(
        base_model=model_info["base_model"],
        adapter_path=model_info["adapter_path"],
        is_loaded=model_info["is_loaded"],
        device=model_info["device"]
    )


@app.post("/generate", response_model=GenerationResponse, tags=["Generation"])
async def generate(request: GenerationRequest):
    """Generate SQL from natural language question."""
    try:
        response = generate_sql(
            question=request.question,
            schema_summary=request.schema_summary,
            retrieved_schema_snippet=request.retrieved_schema_snippet,
            dialect=request.dialect,
            max_tokens=request.max_tokens,
            temperature=request.temperature,
            top_p=request.top_p,
            top_k=request.top_k,
            do_sample=request.do_sample,
            num_beams=request.num_beams
        )
        return response
    except Exception as e:
        logger.error(f"Generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/generate-stream", tags=["Generation"])
async def generate_stream(request: GenerationRequest):
    """Streaming generation endpoint."""
    import asyncio
    
    async def generate_tokens():
        try:
            # Build prompt
            prompt = build_prompt(
                question=request.question,
                schema_summary=request.schema_summary,
                retrieved_schema_snippet=request.retrieved_schema_snippet,
                dialect=request.dialect
            )
            
            messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt}
            ]
            
            text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
            inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=2048)
            
            if torch.cuda.is_available():
                inputs = {k: v.cuda() for k, v in inputs.items()}
            
            # Generate token by token
            generated_ids = inputs.input_ids[0]
            
            for _ in range(request.max_tokens):
                outputs = model.forward(
                    input_ids=generated_ids.unsqueeze(0),
                    use_cache=True
                )
                next_token_logits = outputs.logits[0, -1, :]
                
                # Apply temperature and sampling
                if request.temperature > 0:
                    next_token_logits = next_token_logits / request.temperature
                
                # Apply top-k
                if request.top_k > 0:
                    top_k = min(request.top_k, next_token_logits.size(-1))
                    indices = torch.argsort(next_token_logits, descending=True)[top_k:]
                    next_token_logits[indices] = float('-inf')
                
                # Apply top-p
                if request.top_p < 1.0:
                    sorted_logits, sorted_indices = torch.sort(next_token_logits, descending=True)
                    cumulative_probs = torch.cumsum(torch.softmax(sorted_logits, dim=-1), dim=-1)
                    sorted_indices_to_remove = cumulative_probs > request.top_p
                    next_token_logits[sorted_indices[sorted_indices_to_remove]] = float('-inf')
                
                # Sample
                probs = torch.softmax(next_token_logits, dim=-1)
                if request.do_sample:
                    next_token = torch.multinomial(probs, num_samples=1)
                else:
                    next_token = torch.argmax(probs, dim=-1, keepdim=True)
                
                generated_ids = torch.cat([generated_ids, next_token])
                
                # Check for EOS
                if next_token.item() == tokenizer.eos_token_id:
                    break
                
                # Yield token
                decoded = tokenizer.decode(next_token.item(), skip_special_tokens=True)
                yield f"data: {json.dumps({'token': decoded})}\n\n"
                
                await asyncio.sleep(0)  # Allow other tasks to run
            
            yield "data: [DONE]\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return StreamingResponse(
        generate_tokens(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )


@app.post("/reload-model", tags=["Model"])
async def reload_model(
    model_path: str,
    adapter_path: Optional[str] = None,
    background_tasks: BackgroundTasks = None
):
    """Reload the model."""
    try:
        load_model(model_path, adapter_path)
        return {"status": "success", "message": "Model reloaded"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/unload-model", tags=["Model"])
async def unload_model():
    """Unload the model to free memory."""
    global model, tokenizer, model_info
    
    del model
    del tokenizer
    
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    
    model_info["is_loaded"] = False
    
    return {"status": "success", "message": "Model unloaded"}


# ============== Main ==============

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="NL2SQL Inference Server")
    parser.add_argument(
        "--model",
        type=str,
        default="./output/final",
        help="Path to model directory"
    )
    parser.add_argument(
        "--adapter",
        type=str,
        default=None,
        help="Path to LoRA adapter (optional)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Server port"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Server host"
    )
    parser.add_argument(
        "--device",
        type=str,
        default="auto",
        help="Device to use (auto/cpu/cuda)"
    )
    parser.add_argument(
        "--no-load",
        action="store_true",
        help="Don't load model on startup"
    )
    parser.add_argument(
        "--flash-attention",
        action="store_true",
        help="Use flash attention (requires compatible GPU)"
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Load model if requested
    if not args.no_load:
        load_model(
            model_path=args.model,
            adapter_path=args.adapter,
            device=args.device,
            use_flash_attention=args.flash_attention
        )
    
    # Start server
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level="info"
    )


if __name__ == "__main__":
    main()