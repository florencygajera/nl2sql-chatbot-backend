"""
NL2SQL API Integration Routes

This module adds new API routes that use the enhanced NL2SQL orchestrator.
These routes can be used alongside or as a replacement for the existing chat endpoint.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from app.services.nl2sql_orchestrator import (
    OrchestratorConfig,
    get_orchestrator,
    OrchestratorResult
)
from app.core.config import get_settings
from app.db.session import get_current_dialect

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter(prefix="/nl2sql", tags=["NL2SQL"])


# ============== Request/Response Models ==============

class NL2SQLRequest(BaseModel):
    """Request model for NL2SQL generation."""
    question: str = Field(..., description="Natural language question")
    return_sql_only: bool = Field(
        default=False, 
        description="If true, only return SQL without executing"
    )
    max_rows: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Maximum rows to return"
    )
    temperature: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=2.0,
        description="LLM temperature (optional)"
    )


class TableInfo(BaseModel):
    """Table information in response."""
    name: str
    columns: List[str]
    primary_key: Optional[str] = None
    foreign_keys: List[Dict[str, str]] = []


class NL2SQLResponse(BaseModel):
    """Response model for NL2SQL."""
    success: bool
    question: str
    sql_query: str
    dialect: str
    data: Optional[List[Dict]] = None
    column_names: Optional[List[str]] = None
    error: Optional[str] = None
    warnings: List[str] = []
    timing_ms: Dict[str, float] = {}


class SchemaInfoResponse(BaseModel):
    """Schema information response."""
    dialect: str
    table_count: int
    tables: List[str]


# ============== Endpoints ==============

@router.post(
    "/generate",
    response_model=NL2SQLResponse,
    summary="Generate and execute SQL from natural language",
    description="Transform a natural language question into SQL using the local LLM model"
)
async def generate_sql(request: NL2SQLRequest):
    """
    Generate SQL from natural language question.
    
    This endpoint:
    1. Retrieves relevant schema using TF-IDF
    2. Generates SQL using the local NL2SQL model
    3. Validates SQL using sqlglot
    4. Executes the query (if not return_sql_only)
    5. Returns results or SQL
    """
    try:
        # Get orchestrator with config
        config = OrchestratorConfig(
            llm_service_url=settings.LLM_SERVICE_URL or "http://localhost:8000",
            max_rows=request.max_rows
        )
        
        orchestrator = get_orchestrator(config)
        
        # Process the question
        result = orchestrator.process(
            question=request.question,
            return_sql_only=request.return_sql_only
        )
        
        # Convert to response
        return NL2SQLResponse(
            success=result.success,
            question=result.question,
            sql_query=result.sql_query,
            dialect=result.dialect,
            data=result.data,
            column_names=result.column_names,
            error=result.error,
            warnings=result.warnings,
            timing_ms={
                "retrieval_ms": result.retrieval_time_ms,
                "generation_ms": result.generation_time_ms,
                "execution_ms": result.execution_time_ms,
                "total_ms": result.retrieval_time_ms + result.generation_time_ms + result.execution_time_ms
            }
        )
        
    except Exception as e:
        logger.exception(f"NL2SQL generate error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post(
    "/generate-sql-only",
    response_model=NL2SQLResponse,
    summary="Generate SQL only (no execution)"
)
async def generate_sql_only(request: NL2SQLRequest):
    """
    Generate SQL only, without execution.
    
    Useful for previewing the SQL before execution.
    """
    request.return_sql_only = True
    return await generate_sql(request)


@router.get(
    "/schema-info",
    response_model=SchemaInfoResponse,
    summary="Get current database schema info"
)
async def get_schema_info():
    """
    Get information about the currently connected database schema.
    """
    try:
        from app.db.session import get_schema_summary, get_schema_catalog
        
        dialect = get_current_dialect()
        schema_text = get_schema_summary()
        
        # Parse table count
        table_count = schema_text.count("Table:") if schema_text else 0
        
        # Extract table names
        import re
        tables = re.findall(r'Table:\s*([^\n]+)', schema_text or "") if schema_text else []
        tables = [t.strip().strip('"').strip("'") for t in tables]
        
        return SchemaInfoResponse(
            dialect=dialect,
            table_count=table_count,
            tables=tables
        )
        
    except Exception as e:
        logger.error(f"Schema info error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get(
    "/llm-health",
    summary="Check LLM service health"
)
async def check_llm_health():
    """
    Check if the local LLM service is available and healthy.
    """
    from app.llm.local_client import get_llm_client_instance
    
    try:
        client = get_llm_client_instance(settings.LLM_SERVICE_URL or "http://localhost:8000")
        is_healthy = client.health_check()
        
        return {
            "available": is_healthy,
            "url": settings.LLM_SERVICE_URL or "http://localhost:8000"
        }
        
    except Exception as e:
        return {
            "available": False,
            "error": str(e),
            "url": settings.LLM_SERVICE_URL or "http://localhost:8000"
        }


# ============== Configuration Update ==============

def update_config():
    """Add LLM service URL to config if not present."""
    # This will be called at startup to ensure config is set
    pass