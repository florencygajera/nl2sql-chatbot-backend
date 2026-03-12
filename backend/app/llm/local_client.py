"""
Local NL2SQL LLM Client

This module provides a client for the local NL2SQL inference server.
It replaces the Ollama client for fully local, offline operation.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


# ============== Configuration ==============

@dataclass
class LLMConfig:
    """Configuration for the local LLM service."""
    base_url: str = "http://localhost:8000"
    timeout: int = 120
    max_retries: int = 3
    default_max_tokens: int = 512
    default_temperature: float = 0.2


# ============== Response Models ==============

@dataclass
class LLMResponse:
    """Response from the LLM service."""
    raw_text: str
    parsed: Optional[Dict[str, Any]] = None
    errors: List[str] = field(default_factory=list)
    model_info: Dict[str, str] = field(default_factory=dict)
    timing: Dict[str, float] = field(default_factory=dict)
    success: bool = False


@dataclass 
class GeneratedSQL:
    """Structured SQL generation result."""
    sql_query: str
    dialect: str
    confidence_score: float
    referenced_tables: List[str] = field(default_factory=list)
    needs_clarification: bool = False
    clarification_question: Optional[str] = None
    raw_response: Optional[str] = None
    errors: List[str] = field(default_factory=list)


# ============== Client ==============

class LocalNL2SQLClient:
    """
    Client for the local NL2SQL inference server.
    
    This client connects to the FastAPI server running the trained model.
    """
    
    def __init__(self, config: Optional[LLMConfig] = None):
        self.config = config or LLMConfig()
        self._session = requests.Session()
    
    def _make_request(
        self,
        endpoint: str,
        data: Dict[str, Any],
        timeout: Optional[int] = None
    ) -> Dict[str, Any]:
        """Make a request to the LLM service."""
        url = f"{self.config.base_url}{endpoint}"
        timeout = timeout or self.config.timeout
        
        for attempt in range(self.config.max_retries):
            try:
                response = self._session.post(
                    url,
                    json=data,
                    timeout=timeout
                )
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout (attempt {attempt + 1}/{self.config.max_retries})")
                if attempt == self.config.max_retries - 1:
                    raise
                    
            except requests.exceptions.ConnectionError as e:
                logger.error(f"Connection error: {e}")
                raise RuntimeError(
                    f"Cannot connect to LLM service at {self.config.base_url}. "
                    "Make sure the service is running."
                ) from e
                    
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error: {e}")
                raise
                
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                raise
        
        raise RuntimeError("Max retries exceeded")
    
    def health_check(self) -> bool:
        """Check if the LLM service is healthy."""
        try:
            response = self._session.get(
                f"{self.config.base_url}/health",
                timeout=5
            )
            return response.status_code == 200
        except Exception:
            return False
    
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the loaded model."""
        try:
            return self._make_request("/model-info", {})
        except Exception as e:
            logger.warning(f"Could not get model info: {e}")
            return {"error": str(e)}
    
    def generate(
        self,
        question: str,
        schema_summary: str,
        retrieved_schema_snippet: str,
        dialect: str = "postgres",
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None
    ) -> LLMResponse:
        """
        Generate SQL from natural language question.
        
        Args:
            question: Natural language question
            schema_summary: Full schema summary
            retrieved_schema_snippet: TF-IDF retrieved schema
            dialect: SQL dialect (postgres, mysql, mssql, sqlite, oracle)
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            
        Returns:
            LLMResponse with raw text and parsed result
        """
        data = {
            "question": question,
            "schema_summary": schema_summary,
            "retrieved_schema_snippet": retrieved_schema_snippet,
            "dialect": dialect,
            "max_tokens": max_tokens or self.config.default_max_tokens,
            "temperature": temperature or self.config.default_temperature
        }
        
        try:
            response = self._make_request("/generate", data)
            
            return LLMResponse(
                raw_text=response.get("raw_text", ""),
                parsed=response.get("parsed"),
                errors=response.get("errors", []),
                model_info=response.get("model_info", {}),
                timing=response.get("timing", {}),
                success=True
            )
            
        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            return LLMResponse(
                raw_text="",
                errors=[str(e)],
                success=False
            )
    
    def generate_sql(
        self,
        question: str,
        schema_summary: str,
        retrieved_schema_snippet: str,
        dialect: str = "postgres",
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None
    ) -> GeneratedSQL:
        """
        Generate SQL with structured output.
        
        Args:
            question: Natural language question
            schema_summary: Full schema summary
            retrieved_schema_snippet: TF-IDF retrieved schema
            dialect: SQL dialect
            max_tokens: Maximum tokens to generate
            temperature: Sampling temperature
            
        Returns:
            GeneratedSQL with structured result
        """
        response = self.generate(
            question=question,
            schema_summary=schema_summary,
            retrieved_schema_snippet=retrieved_schema_snippet,
            dialect=dialect,
            max_tokens=max_tokens,
            temperature=temperature
        )
        
        if not response.success:
            return GeneratedSQL(
                sql_query="",
                dialect=dialect,
                confidence_score=0.0,
                errors=response.errors,
                raw_response=response.raw_text
            )
        
        # Parse the response
        parsed = response.parsed
        
        if not parsed:
            # Try to extract SQL from raw text
            import re
            sql_match = re.search(
                r'(SELECT[\s\S]*?)(?:;|$)',
                response.raw_text,
                re.IGNORECASE
            )
            sql_query = sql_match.group(1).strip() if sql_match else ""
            
            return GeneratedSQL(
                sql_query=sql_query,
                dialect=dialect,
                confidence_score=0.0,
                errors=["Could not parse JSON response"] + response.errors,
                raw_response=response.raw_text
            )
        
        return GeneratedSQL(
            sql_query=parsed.get("sql_query", ""),
            dialect=parsed.get("dialect", dialect),
            confidence_score=parsed.get("confidence_score", 0.0),
            referenced_tables=parsed.get("referenced_tables", []),
            needs_clarification=parsed.get("needs_clarification", False),
            clarification_question=parsed.get("clarification_question"),
            raw_response=response.raw_text,
            errors=response.errors
        )
    
    def reload_model(
        self,
        model_path: str,
        adapter_path: Optional[str] = None
    ) -> bool:
        """Reload the model."""
        try:
            params = {"model_path": model_path}
            if adapter_path:
                params["adapter_path"] = adapter_path
            
            response = self._make_request("/reload-model", params)
            return response.get("status") == "success"
        except Exception as e:
            logger.error(f"Model reload failed: {e}")
            return False
    
    def unload_model(self) -> bool:
        """Unload the model to free memory."""
        try:
            response = self._make_request("/unload-model", {})
            return response.get("status") == "success"
        except Exception:
            return False


# ============== Fallback Client ==============

class FallbackSQLGenerator:
    """
    Fallback SQL generator when LLM service is unavailable.
    
    Uses simple template matching for basic queries.
    """
    
    def __init__(self):
        self.keywords = {
            "count": ["how many", "total number", "number of", "count"],
            "list": ["list", "show", "get all", "display"],
            "sum": ["sum", "total", "amount"],
            "average": ["average", "avg", "mean"],
            "min": ["minimum", "lowest", "smallest", "min"],
            "max": ["maximum", "highest", "largest", "max"],
            "top": ["top", "first", "leading"],
            "recent": ["recent", "latest", "newest", "last"],
            "filter": ["where", "with", "having", "filter"]
        }
    
    def generate(
        self,
        question: str,
        schema_summary: str,
        dialect: str = "postgres"
    ) -> GeneratedSQL:
        """Generate simple SQL using template matching."""
        question_lower = question.lower()
        
        # Try to identify query type
        query_type = "select"
        for qtype, keywords in self.keywords.items():
            if any(kw in question_lower for kw in keywords):
                query_type = qtype
                break
        
        # Extract potential table name from schema
        import re
        table_match = re.search(r'Table:\s*(\w+)', schema_summary)
        table_name = table_match.group(1) if table_match else "table"
        
        # Extract potential column
        col_match = re.search(r'-\s*(\w+)', schema_summary)
        column = col_match.group(1) if col_match else "*"
        
        # Build simple SQL
        if query_type == "count":
            sql = f"SELECT COUNT(*) FROM {table_name}"
        elif query_type == "sum":
            sql = f"SELECT SUM({column}) FROM {table_name}"
        elif query_type == "average":
            sql = f"SELECT AVG({column}) FROM {table_name}"
        elif query_type == "min":
            sql = f"SELECT MIN({column}) FROM {table_name}"
        elif query_type == "max":
            sql = f"SELECT MAX({column}) FROM {table_name}"
        elif query_type == "top":
            sql = f"SELECT TOP 10 * FROM {table_name}" if dialect == "mssql" else f"SELECT * FROM {table_name} LIMIT 10"
        elif query_type == "recent":
            sql = f"SELECT * FROM {table_name} ORDER BY created_at DESC LIMIT 10"
        else:
            sql = f"SELECT * FROM {table_name} LIMIT 1000"
        
        return GeneratedSQL(
            sql_query=sql,
            dialect=dialect,
            confidence_score=0.3,  # Low confidence for fallback
            referenced_tables=[table_name],
            errors=["Using fallback SQL generator - LLM service unavailable"]
        )


# ============== Client Factory ==============

def get_llm_client(
    base_url: str = "http://localhost:8000",
    use_fallback: bool = True
) -> LocalNL2SQLClient:
    """
    Get an LLM client, with fallback option.
    
    Args:
        base_url: URL of the LLM service
        use_fallback: If True, wraps in a client that falls back to template matching
        
    Returns:
        LLM client instance
    """
    client = LocalNL2SQLClient(LLMConfig(base_url=base_url))
    
    if use_fallback:
        # Check if service is available
        if not client.health_check():
            logger.warning(
                f"LLM service not available at {base_url}. "
                "Using fallback SQL generator."
            )
            return FallbackSQLGenerator()
    
    return client


# ============== Singleton Instance ==============

# Global client instance
_llm_client: Optional[LocalNL2SQLClient] = None


def get_llm_client_instance(
    base_url: str = "http://localhost:8000"
) -> LocalNL2SQLClient:
    """Get the global LLM client instance."""
    global _llm_client
    
    if _llm_client is None:
        _llm_client = get_llm_client(base_url)
    
    return _llm_client