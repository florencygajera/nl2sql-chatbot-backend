"""
NL2SQL Orchestrator - Complete Pipeline Integration

This module orchestrates the full NL2SQL pipeline:
1. Schema retrieval (TF-IDF based)
2. LLM generation (local model)
3. SQL validation (sqlglot)
4. Safety checks (SQL Guard)
5. Row limit enforcement
6. Query execution
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from app.db.session import (
    get_schema_summary,
    get_schema_catalog,
    get_schema_for_tables,
    get_current_dialect,
    get_foreign_keys_catalog
)
from app.db.schema_tfidf import retrieve_schema, get_schema_retriever, build_schema_text_from_catalog
from app.db.sql_validator import validate_and_fix_sql, SQLValidationResult
from app.security.sql_guard import validate_and_sanitize, SQLGuardError
from app.llm.local_client import get_llm_client_instance, GeneratedSQL, LocalNL2SQLClient
from app.services.query_executor import execute_query, QueryResult, QueryExecutionError


logger = logging.getLogger(__name__)


# ============== Configuration ==============

@dataclass
class OrchestratorConfig:
    """Configuration for the NL2SQL orchestrator."""
    llm_service_url: str = "http://localhost:8000"
    max_rows: int = 1000
    tfidf_top_k: int = 5
    max_retries: int = 2
    use_fallback_on_llm_error: bool = True


# ============== Response Models ==============

@dataclass
class OrchestratorResult:
    """Result of the orchestrator pipeline."""
    success: bool
    question: str
    sql_query: str
    dialect: str
    data: Optional[List[Dict]] = None
    column_names: Optional[List[str]] = None
    error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    validation_errors: List[str] = field(default_factory=list)
    execution_time_ms: float = 0.0
    generation_time_ms: float = 0.0
    retrieval_time_ms: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON response."""
        return {
            "success": self.success,
            "question": self.question,
            "sql_query": self.sql_query,
            "dialect": self.dialect,
            "data": self.data,
            "column_names": self.column_names,
            "error": self.error,
            "warnings": self.warnings,
            "validation_errors": self.validation_errors,
            "execution_time_ms": self.execution_time_ms,
            "generation_time_ms": self.generation_time_ms,
            "retrieval_time_ms": self.retrieval_time_ms
        }


# ============== Main Orchestrator ==============

class NL2SQLOrchestrator:
    """
    Main orchestrator for the NL2SQL pipeline.
    
    This class coordinates all components to transform a natural language
    question into SQL, validate it, and execute it against the database.
    """
    
    def __init__(self, config: Optional[OrchestratorConfig] = None):
        self.config = config or OrchestratorConfig()
        self._llm_client: Optional[LocalNL2SQLClient] = None
    
    @property
    def llm_client(self) -> LocalNL2SQLClient:
        """Get or create the LLM client."""
        if self._llm_client is None:
            self._llm_client = get_llm_client_instance(self.config.llm_service_url)
        return self._llm_client
    
    def process(
        self,
        question: str,
        return_sql_only: bool = False
    ) -> OrchestratorResult:
        """
        Process a natural language question through the full pipeline.
        
        Args:
            question: Natural language question
            return_sql_only: If True, only return SQL without executing
            
        Returns:
            OrchestratorResult with the result
        """
        start_time = time.time()
        dialect = get_current_dialect()
        
        try:
            # Step 1: Get schema
            retrieval_start = time.time()
            schema_summary = get_schema_summary()
            schema_catalog = get_schema_catalog()
            fk_catalog = get_foreign_keys_catalog()
            retrieval_time = time.time() - retrieval_start
            
            if not schema_summary:
                return OrchestratorResult(
                    success=False,
                    question=question,
                    sql_query="",
                    dialect=dialect,
                    error="No schema available. Please connect to a database first.",
                    retrieval_time_ms=retrieval_time * 1000
                )
            
            # Step 2: TF-IDF schema retrieval
            retrieved_snippet, retrieved_tables = retrieve_schema(
                question=question,
                schema_text=schema_summary,
                top_k=self.config.tfidf_top_k
            )
            
            logger.info(f"Retrieved {len(retrieved_tables)} tables: {retrieved_tables}")
            
            # Step 3: Generate SQL with LLM
            generation_start = time.time()
            sql_result = self._generate_sql(
                question=question,
                schema_summary=schema_summary,
                retrieved_schema_snippet=retrieved_snippet,
                dialect=dialect
            )
            generation_time = time.time() - generation_start
            
            if not sql_result.sql_query:
                return OrchestratorResult(
                    success=False,
                    question=question,
                    sql_query="",
                    dialect=dialect,
                    error=sql_result.errors[0] if sql_result.errors else "Failed to generate SQL",
                    warnings=sql_result.errors,
                    retrieval_time_ms=retrieval_time * 1000,
                    generation_time_ms=generation_time * 1000
                )
            
            # Step 4: Validate SQL
            validation_result = self._validate_sql(
                sql=sql_result.sql_query,
                dialect=dialect,
                schema_tables=retrieved_tables
            )
            
            if not validation_result.is_valid:
                return OrchestratorResult(
                    success=False,
                    question=question,
                    sql_query=sql_result.sql_query,
                    dialect=dialect,
                    error="SQL validation failed",
                    validation_errors=validation_result.errors,
                    warnings=validation_result.warnings,
                    retrieval_time_ms=retrieval_time * 1000,
                    generation_time_ms=generation_time * 1000
                )
            
            validated_sql = validation_result.sanitized_sql or sql_result.sql_query
            
            # Step 5: Execute or return SQL
            if return_sql_only:
                return OrchestratorResult(
                    success=True,
                    question=question,
                    sql_query=validated_sql,
                    dialect=dialect,
                    warnings=validation_result.warnings,
                    retrieval_time_ms=retrieval_time * 1000,
                    generation_time_ms=generation_time * 1000
                )
            
            # Execute query
            execution_start = time.time()
            query_result = self._execute_sql(validated_sql, dialect)
            execution_time = time.time() - execution_start
            
            return OrchestratorResult(
                success=query_result.success,
                question=question,
                sql_query=validated_sql,
                dialect=dialect,
                data=query_result.data,
                column_names=query_result.column_names,
                error=query_result.error,
                warnings=validation_result.warnings + query_result.warnings,
                execution_time_ms=execution_time * 1000,
                generation_time_ms=generation_time * 1000,
                retrieval_time_ms=retrieval_time * 1000
            )
            
        except Exception as e:
            logger.exception(f"Orchestrator error: {e}")
            total_time = time.time() - start_time
            
            return OrchestratorResult(
                success=False,
                question=question,
                sql_query="",
                dialect=dialect,
                error=str(e),
                execution_time_ms=total_time * 1000
            )
    
    def _generate_sql(
        self,
        question: str,
        schema_summary: str,
        retrieved_schema_snippet: str,
        dialect: str
    ) -> GeneratedSQL:
        """Generate SQL using the LLM."""
        for attempt in range(self.config.max_retries):
            try:
                result = self.llm_client.generate_sql(
                    question=question,
                    schema_summary=schema_summary,
                    retrieved_schema_snippet=retrieved_schema_snippet,
                    dialect=dialect
                )
                
                if result.sql_query or attempt == self.config.max_retries - 1:
                    return result
                    
            except Exception as e:
                logger.warning(f"LLM generation attempt {attempt + 1} failed: {e}")
                if attempt == self.config.max_retries - 1:
                    return GeneratedSQL(
                        sql_query="",
                        dialect=dialect,
                        confidence_score=0.0,
                        errors=[f"LLM error: {str(e)}"]
                    )
        
        return GeneratedSQL(
            sql_query="",
            dialect=dialect,
            confidence_score=0.0,
            errors=["Max retries exceeded"]
        )
    
    def _validate_sql(
        self,
        sql: str,
        dialect: str,
        schema_tables: List[str]
    ) -> SQLValidationResult:
        """Validate and sanitize SQL."""
        # First, use SQL Guard for security checks
        try:
            guard_result = validate_and_sanitize(sql, dialect)
            if not guard_result.is_valid:
                return SQLValidationResult(
                    is_valid=False,
                    errors=guard_result.errors
                )
            sql = guard_result.sanitized_sql
        except SQLGuardError as e:
            return SQLValidationResult(
                is_valid=False,
                errors=[str(e)]
            )
        
        # Then use sqlglot for deeper validation
        validated_sql, result = validate_and_fix_sql(
            sql=sql,
            schema_tables=schema_tables,
            db_dialect=dialect,
            max_rows=self.config.max_rows
        )
        
        return result
    
    def _execute_sql(
        self,
        sql: str,
        dialect: str
    ) -> QueryResult:
        """Execute SQL and return results."""
        try:
            return execute_query(sql, dialect)
        except QueryExecutionError as e:
            return QueryResult(
                success=False,
                data=None,
                error=str(e)
            )


# ============== Singleton ==============

_orchestrator: Optional[NL2SQLOrchestrator] = None


def get_orchestrator(config: Optional[OrchestratorConfig] = None) -> NL2SQLOrchestrator:
    """Get the global orchestrator instance."""
    global _orchestrator
    
    if _orchestrator is None or config is not None:
        _orchestrator = NL2SQLOrchestrator(config)
    
    return _orchestrator


def process_question(
    question: str,
    llm_url: str = "http://localhost:8000",
    return_sql_only: bool = False,
    max_rows: int = 1000
) -> OrchestratorResult:
    """
    Convenience function to process a question.
    
    Args:
        question: Natural language question
        llm_url: URL of the LLM service
        return_sql_only: If True, only return SQL
        max_rows: Maximum rows to return
        
    Returns:
        OrchestratorResult
    """
    config = OrchestratorConfig(
        llm_service_url=llm_url,
        max_rows=max_rows
    )
    
    orchestrator = get_orchestrator(config)
    return orchestrator.process(question, return_sql_only)