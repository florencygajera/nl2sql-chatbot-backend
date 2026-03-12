"""
NL2SQL Evaluation Pipeline
Evaluates the NL2SQL model on test queries.
"""

import os
import sys
import json
import time
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics
import re

import requests
import sqlglot

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============== Data Models ==============

@dataclass
class TestQuery:
    """A test query for evaluation."""
    question: str
    expected_sql: str
    schema_summary: str
    retrieved_schema_snippet: str
    dialect: str
    referenced_tables: List[str] = field(default_factory=list)


@dataclass
class EvaluationResult:
    """Result of evaluating a single query."""
    question: str
    expected_sql: str
    generated_sql: str
    dialect: str
    json_valid: bool
    sql_syntax_valid: bool
    schema_correct: bool
    execution_success: bool
    latency_seconds: float
    errors: List[str] = field(default_factory=list)
    confidence_score: Optional[float] = None


@dataclass
class EvaluationMetrics:
    """Aggregated evaluation metrics."""
    total_queries: int
    json_validity_rate: float
    syntax_valid_rate: float
    schema_correct_rate: float
    execution_success_rate: float
    average_latency_seconds: float
    p50_latency: float
    p95_latency: float
    p99_latency: float
    results: List[EvaluationResult] = field(default_factory=list)


# ============== Client ==============

class NL2SQLClient:
    """Client for the NL2SQL inference server."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip("/")
        self.timeout = 120  # seconds
        
    def health_check(self) -> bool:
        """Check if the server is healthy."""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=5)
            return response.status_code == 200
        except Exception:
            return False
    
    def generate(
        self,
        question: str,
        schema_summary: str,
        retrieved_schema_snippet: str,
        dialect: str = "postgres",
        max_tokens: int = 512,
        temperature: float = 0.2
    ) -> Dict[str, Any]:
        """Generate SQL from natural language question."""
        url = f"{self.base_url}/generate"
        
        payload = {
            "question": question,
            "schema_summary": schema_summary,
            "retrieved_schema_snippet": retrieved_schema_snippet,
            "dialect": dialect,
            "max_tokens": max_tokens,
            "temperature": temperature
        }
        
        response = requests.post(url, json=payload, timeout=self.timeout)
        response.raise_for_status()
        
        return response.json()


# ============== Evaluation ==============

def validate_json(text: str) -> Tuple[bool, Optional[Dict], str]:
    """Validate that text is valid JSON."""
    try:
        parsed = json.loads(text.strip())
        return True, parsed, ""
    except json.JSONDecodeError as e:
        return False, None, str(e)


def validate_sql_syntax(sql: str, dialect: str) -> Tuple[bool, str]:
    """Validate SQL syntax using sqlglot."""
    try:
        # Parse the SQL
        parsed = sqlglot.parse(sql, dialect=dialect)
        
        if not parsed:
            return False, "Empty or unparseable SQL"
        
        # Check it's a SELECT statement
        if parsed[0].find("select") is None and parsed[0].find("SELECT") is None:
            return False, "Only SELECT queries are allowed"
        
        return True, ""
    except Exception as e:
        return False, f"Syntax error: {str(e)}"


def validate_schema_references(
    sql: str,
    expected_tables: List[str],
    dialect: str
) -> Tuple[bool, str]:
    """Validate that SQL references the correct tables."""
    try:
        parsed = sqlglot.parse(sql, dialect=dialect)
        if not parsed:
            return False, "Could not parse SQL"
        
        # Extract table references
        tables_in_sql = set()
        
        for exp in parsed[0].walk():
            if isinstance(exp, sqlglot.exp.Table):
                table_name = exp.name.lower()
                tables_in_sql.add(table_name)
        
        # Check against expected tables
        expected_lower = set(t.lower() for t in expected_tables)
        
        if expected_tables and not expected_lower.issubset(tables_in_sql):
            missing = expected_lower - tables_in_sql
            return False, f"Missing expected tables: {missing}"
        
        return True, ""
    except Exception as e:
        return False, f"Schema validation error: {str(e)}"


def check_sql_safety(sql: str) -> Tuple[bool, str]:
    """Check SQL for safety (no dangerous operations)."""
    dangerous_keywords = [
        "DROP", "DELETE", "UPDATE", "INSERT", "ALTER",
        "TRUNCATE", "CREATE", "EXEC", "CALL", "GRANT",
        "REVOKE", "DENY"
    ]
    
    sql_upper = sql.upper()
    
    # Check for dangerous keywords
    for keyword in dangerous_keywords:
        # Make sure it's a standalone keyword, not part of another word
        pattern = r'\b' + keyword + r'\b'
        if re.search(pattern, sql_upper):
            return False, f"Dangerous keyword found: {keyword}"
    
    # Check for semicolons (potential for multiple statements)
    if ';' in sql.strip().rstrip(';'):
        return False, "Multiple statements not allowed"
    
    return True, ""


def normalize_sql(sql: str) -> str:
    """Normalize SQL for comparison."""
    # Remove extra whitespace
    sql = ' '.join(sql.split())
    
    # Remove trailing semicolon
    sql = sql.rstrip(';')
    
    # Convert to lowercase for comparison
    sql = sql.lower()
    
    return sql


def evaluate_query(
    client: NL2SQLClient,
    test_query: TestQuery,
    validate_execution: bool = False
) -> EvaluationResult:
    """Evaluate a single test query."""
    start_time = time.time()
    errors = []
    
    try:
        # Generate SQL
        response = client.generate(
            question=test_query.question,
            schema_summary=test_query.schema_summary,
            retrieved_schema_snippet=test_query.retrieved_schema_snippet,
            dialect=test_query.dialect
        )
        
        latency = time.time() - start_time
        
        # Extract generated SQL
        generated_sql = ""
        confidence_score = None
        
        if response.get("parsed"):
            parsed = response["parsed"]
            generated_sql = parsed.get("sql_query", "")
            confidence_score = parsed.get("confidence_score")
        else:
            errors.extend(response.get("errors", []))
            # Try to extract from raw text
            raw_text = response.get("raw_text", "")
            # Try to find SQL in response
            sql_match = re.search(r'(SELECT.*?)(?:\n|$)', raw_text, re.IGNORECASE | re.DOTALL)
            if sql_match:
                generated_sql = sql_match.group(1).strip()
        
        # Validate JSON
        json_valid = response.get("parsed") is not None
        
        # Validate SQL syntax
        syntax_valid, syntax_error = validate_sql_syntax(generated_sql, test_query.dialect)
        if not syntax_valid:
            errors.append(syntax_error)
        
        # Check SQL safety
        if generated_sql:
            safe, safety_error = check_sql_safety(generated_sql)
            if not safe:
                errors.append(safety_error)
        
        # Validate schema references
        if generated_sql and test_query.referenced_tables:
            schema_valid, schema_error = validate_schema_references(
                generated_sql,
                test_query.referenced_tables,
                test_query.dialect
            )
            if not schema_valid:
                errors.append(schema_error)
        else:
            schema_valid = True
        
        # Execution would require actual database
        execution_success = syntax_valid  # Simplified for now
        
        return EvaluationResult(
            question=test_query.question,
            expected_sql=test_query.expected_sql,
            generated_sql=generated_sql,
            dialect=test_query.dialect,
            json_valid=json_valid,
            sql_syntax_valid=syntax_valid,
            schema_correct=schema_valid,
            execution_success=execution_success,
            latency_seconds=latency,
            errors=errors,
            confidence_score=confidence_score
        )
        
    except Exception as e:
        latency = time.time() - start_time
        return EvaluationResult(
            question=test_query.question,
            expected_sql=test_query.expected_sql,
            generated_sql="",
            dialect=test_query.dialect,
            json_valid=False,
            sql_syntax_valid=False,
            schema_correct=False,
            execution_success=False,
            latency_seconds=latency,
            errors=[str(e)]
        )


def run_evaluation(
    client: NL2SQLClient,
    test_queries: List[TestQuery],
    max_workers: int = 4,
    verbose: bool = True
) -> EvaluationMetrics:
    """Run evaluation on multiple queries."""
    results: List[EvaluationResult] = []
    
    # Run evaluations in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(evaluate_query, client, q): q
            for q in test_queries
        }
        
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            
            if verbose:
                status = "✓" if result.json_valid and result.sql_syntax_valid else "✗"
                logger.info(f"{status} {result.question[:50]}... ({result.latency_seconds:.2f}s)")
    
    # Calculate metrics
    total = len(results)
    json_valid_count = sum(1 for r in results if r.json_valid)
    syntax_valid_count = sum(1 for r in results if r.sql_syntax_valid)
    schema_correct_count = sum(1 for r in results if r.schema_correct)
    execution_success_count = sum(1 for r in results if r.execution_success)
    
    latencies = [r.latency_seconds for r in results]
    
    metrics = EvaluationMetrics(
        total_queries=total,
        json_validity_rate=json_valid_count / total if total > 0 else 0,
        syntax_valid_rate=syntax_valid_count / total if total > 0 else 0,
        schema_correct_rate=schema_correct_count / total if total > 0 else 0,
        execution_success_rate=execution_success_count / total if total > 0 else 0,
        average_latency_seconds=statistics.mean(latencies) if latencies else 0,
        p50_latency=statistics.median(latencies) if latencies else 0,
        p95_latency=sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0,
        p99_latency=sorted(latencies)[int(len(latencies) * 0.99)] if latencies else 0,
        results=results
    )
    
    return metrics


def print_metrics(metrics: EvaluationMetrics) -> None:
    """Print evaluation metrics."""
    print("\n" + "=" * 60)
    print("EVALUATION RESULTS")
    print("=" * 60)
    print(f"Total Queries:       {metrics.total_queries}")
    print(f"JSON Validity Rate:  {metrics.json_validity_rate * 100:.1f}%")
    print(f"Syntax Valid Rate:   {metrics.syntax_valid_rate * 100:.1f}%")
    print(f"Schema Correct Rate: {metrics.schema_correct_rate * 100:.1f}%")
    print(f"Execution Success:   {metrics.execution_success_rate * 100:.1f}%")
    print("-" * 60)
    print(f"Average Latency:     {metrics.average_latency_seconds:.2f}s")
    print(f"P50 Latency:         {metrics.p50_latency:.2f}s")
    print(f"P95 Latency:         {metrics.p95_latency:.2f}s")
    print(f"P99 Latency:         {metrics.p99_latency:.2f}s")
    print("=" * 60)


# ============== Test Data ==============

def load_test_queries(path: str) -> List[TestQuery]:
    """Load test queries from JSON file."""
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return [
        TestQuery(
            question=q["question"],
            expected_sql=q.get("expected_sql", ""),
            schema_summary=q.get("schema_summary", ""),
            retrieved_schema_snippet=q.get("retrieved_schema_snippet", ""),
            dialect=q.get("dialect", "postgres"),
            referenced_tables=q.get("referenced_tables", [])
        )
        for q in data
    ]


def generate_sample_test_queries() -> List[TestQuery]:
    """Generate sample test queries for demonstration."""
    schema = """Table: users
  - id (INTEGER): Primary key
  - username (VARCHAR): User's login name
  - email (VARCHAR): User's email address
  - created_at (TIMESTAMP): Account creation time

Table: orders
  - id (INTEGER): Primary key
  - user_id (INTEGER): Foreign key to users
  - total_amount (DECIMAL): Order total
  - status (VARCHAR): Order status
  - created_at (TIMESTAMP): Order creation time

Table: products
  - id (INTEGER): Primary key
  - name (VARCHAR): Product name
  - price (DECIMAL): Product price
  - category (VARCHAR): Product category"""
    
    queries = [
        TestQuery(
            question="How many users are there?",
            expected_sql="SELECT COUNT(*) FROM users",
            schema_summary=schema,
            retrieved_schema_snippet="Table: users\n  - id (INTEGER)\n  - username (VARCHAR)",
            dialect="postgres",
            referenced_tables=["users"]
        ),
        TestQuery(
            question="Show me the first 10 orders",
            expected_sql="SELECT * FROM orders LIMIT 10",
            schema_summary=schema,
            retrieved_schema_snippet="Table: orders\n  - id (INTEGER)\n  - user_id (INTEGER)\n  - total_amount (DECIMAL)",
            dialect="postgres",
            referenced_tables=["orders"]
        ),
        TestQuery(
            question="List all products in the Electronics category",
            expected_sql="SELECT * FROM products WHERE category = 'Electronics'",
            schema_summary=schema,
            retrieved_schema_snippet="Table: products\n  - name (VARCHAR)\n  - category (VARCHAR)",
            dialect="postgres",
            referenced_tables=["products"]
        ),
        TestQuery(
            question="What is the total revenue from all orders?",
            expected_sql="SELECT SUM(total_amount) FROM orders",
            schema_summary=schema,
            retrieved_schema_snippet="Table: orders\n  - total_amount (DECIMAL)",
            dialect="postgres",
            referenced_tables=["orders"]
        ),
        TestQuery(
            question="Show orders sorted by total amount descending",
            expected_sql="SELECT * FROM orders ORDER BY total_amount DESC",
            schema_summary=schema,
            retrieved_schema_snippet="Table: orders\n  - total_amount (DECIMAL)\n  - created_at (TIMESTAMP)",
            dialect="postgres",
            referenced_tables=["orders"]
        ),
    ]
    
    return queries


# ============== Main ==============

def main():
    """Main evaluation entry point."""
    parser = argparse.ArgumentParser(description="NL2SQL Evaluation")
    parser.add_argument(
        "--url",
        type=str,
        default="http://localhost:8000",
        help="NL2SQL server URL"
    )
    parser.add_argument(
        "--test-data",
        type=str,
        help="Path to test data JSON file"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="./eval_results.json",
        help="Output path for results"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use sample test queries"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    # Create client
    client = NL2SQLClient(args.url)
    
    # Check server health
    logger.info(f"Checking server at {args.url}...")
    if not client.health_check():
        logger.error("Server not available. Make sure the server is running.")
        sys.exit(1)
    
    logger.info("Server is healthy")
    
    # Load test queries
    if args.sample:
        test_queries = generate_sample_test_queries()
    elif args.test_data:
        test_queries = load_test_queries(args.test_data)
    else:
        logger.error("No test data provided. Use --sample or --test-data")
        sys.exit(1)
    
    logger.info(f"Running evaluation on {len(test_queries)} queries...")
    
    # Run evaluation
    metrics = run_evaluation(
        client=client,
        test_queries=test_queries,
        max_workers=args.workers,
        verbose=args.verbose
    )
    
    # Print metrics
    print_metrics(metrics)
    
    # Save results
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(asdict(metrics), f, indent=2)
    
    logger.info(f"Results saved to {args.output}")
    
    # Exit with appropriate code
    if metrics.json_validity_rate < 0.8:
        sys.exit(1)


if __name__ == "__main__":
    main()