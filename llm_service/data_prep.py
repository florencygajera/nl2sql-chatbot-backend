"""
NL2SQL Data Preparation Pipeline
Generates synthetic training data from schema definitions and combines with Spider dataset.
"""

import json
import os
import random
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import math


@dataclass
class TableInfo:
    """Information about a database table."""
    name: str
    columns: List[Dict[str, str]]  # List of {name, type, description}
    primary_key: Optional[str] = None
    foreign_keys: List[Dict[str, str]] = None  # List of {column, ref_table, ref_column}
    description: str = ""
    
    def __post_init__(self):
        if self.foreign_keys is None:
            self.foreign_keys = []


@dataclass
class SchemaCatalog:
    """Complete database schema catalog."""
    tables: List[TableInfo]
    database_name: str = "unknown"
    dialect: str = "postgres"
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'SchemaCatalog':
        tables = [TableInfo(**t) for t in data.get('tables', [])]
        return cls(
            tables=tables,
            database_name=data.get('database_name', 'unknown'),
            dialect=data.get('dialect', 'postgres')
        )


class SyntheticQueryGenerator:
    """Generate synthetic NL2SQL pairs from schema definitions."""
    
    # Query templates for different SQL patterns
    QUERY_TEMPLATES = {
        "count": [
            ("How many {table} are there?", "SELECT COUNT(*) FROM {table}"),
            ("What's the total count of {table} records?", "SELECT COUNT(*) AS total FROM {table}"),
            ("Show me the number of {table} entries", "SELECT COUNT(*) FROM {table}"),
        ],
        "top_k": [
            ("Show me the first {k} {table}", "SELECT * FROM {table} LIMIT {k}"),
            ("List top {k} {table}", "SELECT * FROM {table} LIMIT {k}"),
            ("Get {k} rows from {table}", "SELECT * FROM {table} LIMIT {k}"),
        ],
        "select_all": [
            ("Show all {table}", "SELECT * FROM {table}"),
            ("List everything in {table}", "SELECT * FROM {table}"),
            ("What data is in {table}?", "SELECT * FROM {table}"),
        ],
        "filter": [
            ("Show {table} where {column} equals {value}", "SELECT * FROM {table} WHERE {column} = '{value}'"),
            ("Find {table} with {column} = {value}", "SELECT * FROM {table} WHERE {column} = '{value}'"),
            ("Get rows from {table} where {column} is {value}", "SELECT * FROM {table} WHERE {column} = '{value}'"),
        ],
        "order_by": [
            ("Show {table} sorted by {column}", "SELECT * FROM {table} ORDER BY {column}"),
            ("List {table} ordered by {column}", "SELECT * FROM {table} ORDER BY {column}"),
            ("Order {table} by {column}", "SELECT * FROM {table} ORDER BY {column}"),
        ],
        "order_by_desc": [
            ("Show {table} sorted by {column} descending", "SELECT * FROM {table} ORDER BY {column} DESC"),
            ("List {table} ordered by {column} (newest first)", "SELECT * FROM {table} ORDER BY {column} DESC"),
            ("Order {table} by {column} descending", "SELECT * FROM {table} ORDER BY {column} DESC"),
        ],
        "group_by": [
            ("Count {table} by {column}", "SELECT {column}, COUNT(*) FROM {table} GROUP BY {column}"),
            ("Group {table} by {column}", "SELECT {column}, COUNT(*) FROM {table} GROUP BY {column}"),
            ("How many per {column} in {table}?", "SELECT {column}, COUNT(*) FROM {table} GROUP BY {column}"),
        ],
        "sum": [
            ("Total {column} in {table}", "SELECT SUM({column}) FROM {table}"),
            ("Sum of {column} across {table}", "SELECT SUM({column}) AS total FROM {table}"),
            ("What is the sum of {column}?", "SELECT SUM({column}) FROM {table}"),
        ],
        "avg": [
            ("Average {column} in {table}", "SELECT AVG({column}) FROM {table}"),
            ("Mean of {column} in {table}", "SELECT AVG({column}) AS average FROM {table}"),
            ("What is the average of {column}?", "SELECT AVG({column}) FROM {table}"),
        ],
        "min": [
            ("Minimum {column} in {table}", "SELECT MIN({column}) FROM {table}"),
            ("Lowest value of {column}", "SELECT MIN({column}) AS minimum FROM {table}"),
            ("Smallest {column} value", "SELECT MIN({column}) FROM {table}"),
        ],
        "max": [
            ("Maximum {column} in {table}", "SELECT MAX({column}) FROM {table}"),
            ("Highest value of {column}", "SELECT MAX({column}) AS maximum FROM {table}"),
            ("Largest {column} value", "SELECT MAX({column}) FROM {table}"),
        ],
        "distinct": [
            ("List unique {column} values", "SELECT DISTINCT {column} FROM {table}"),
            ("Show distinct {column}", "SELECT DISTINCT {column} FROM {table}"),
            ("What unique {column} values exist?", "SELECT DISTINCT {column} FROM {table}"),
        ],
        "between": [
            ("Show {table} where {column} between {a} and {b}", "SELECT * FROM {table} WHERE {column} BETWEEN {a} AND {b}"),
            ("Find {table} with {column} in range {a}-{b}", "SELECT * FROM {table} WHERE {column} BETWEEN {a} AND {b}"),
            ("{column} values from {a} to {b}", "SELECT * FROM {table} WHERE {column} BETWEEN {a} AND {b}"),
        ],
        "like": [
            ("Search {table} for {column} like {pattern}", "SELECT * FROM {table} WHERE {column} LIKE '%{pattern}%'"),
            ("Find {table} where {column} contains {pattern}", "SELECT * FROM {table} WHERE {column} LIKE '%{pattern}%'"),
            ("{column} containing {pattern}", "SELECT * FROM {table} WHERE {column} LIKE '%{pattern}%'"),
        ],
        "is_null": [
            ("Show {table} where {column} is null", "SELECT * FROM {table} WHERE {column} IS NULL"),
            ("Find rows with empty {column}", "SELECT * FROM {table} WHERE {column} IS NULL"),
            ("Records with null {column}", "SELECT * FROM {table} WHERE {column} IS NULL"),
        ],
        "not_null": [
            ("Show {table} where {column} is not null", "SELECT * FROM {table} WHERE {column} IS NOT NULL"),
            ("Find rows with {column} filled", "SELECT * FROM {table} WHERE {column} IS NOT NULL"),
            ("Records with non-null {column}", "SELECT * FROM {table} WHERE {column} IS NOT NULL"),
        ],
    }
    
    # Column value suggestions based on type
    TYPE_VALUES = {
        "int": ["1", "10", "100", "1000"],
        "bigint": ["1", "1000", "10000"],
        "smallint": ["1", "10", "100"],
        "float": ["1.0", "10.5", "100.0"],
        "decimal": ["10.50", "100.25", "999.99"],
        "varchar": ["test", "sample", "value"],
        "text": ["description", "content", "text"],
        "date": ["2024-01-01", "2024-12-31"],
        "timestamp": ["2024-01-01 00:00:00", "2024-12-31 23:59:59"],
        "bool": ["true", "false"],
    }
    
    def __init__(self, schema_catalog: SchemaCatalog):
        self.schema = schema_catalog
        self.generated_pairs: List[Dict] = []
        
    def _get_column_by_type(self, table: TableInfo, types: List[str]) -> Optional[Dict]:
        """Get a column matching one of the specified types."""
        for col in table.columns:
            col_type = col.get('type', '').lower()
            for t in types:
                if t in col_type:
                    return col
        return None
    
    def _get_numeric_columns(self, table: TableInfo) -> List[Dict]:
        """Get numeric columns from a table."""
        numeric_types = ['int', 'bigint', 'smallint', 'float', 'decimal', 'numeric', 'real', 'double']
        return [c for c in table.columns if any(t in c.get('type', '').lower() for t in numeric_types)]
    
    def _get_string_columns(self, table: TableInfo) -> List[Dict]:
        """Get string columns from a table."""
        string_types = ['varchar', 'text', 'char', 'nvarchar', 'ntext']
        return [c for c in table.columns if any(t in c.get('type', '').lower() for t in string_types)]
    
    def _get_all_columns(self, table: TableInfo) -> List[Dict]:
        """Get all columns from a table."""
        return table.columns
    
    def _get_sample_values(self, column: Dict) -> List[str]:
        """Get sample values for a column based on its type."""
        col_type = column.get('type', '').lower()
        for type_key, values in self.TYPE_VALUES.items():
            if type_key in col_type:
                return values
        return ["value"]
    
    def _format_schema_summary(self, table: TableInfo) -> str:
        """Format a table schema as a string."""
        lines = [f"Table: {table.name}"]
        if table.description:
            lines.append(f"Description: {table.description}")
        
        for col in table.columns:
            col_name = col.get('name', '')
            col_type = col.get('type', '')
            col_desc = col.get('description', '')
            lines.append(f"  - {col_name} ({col_type})" + (f": {col_desc}" if col_desc else ""))
        
        if table.primary_key:
            lines.append(f"Primary Key: {table.primary_key}")
        
        if table.foreign_keys:
            for fk in table.foreign_keys:
                lines.append(f"Foreign Key: {fk['column']} -> {fk['ref_table']}.{fk['ref_column']}")
        
        return "\n".join(lines)
    
    def generate_simple_queries(self, max_per_table: int = 50) -> List[Dict]:
        """Generate simple queries for each table."""
        queries = []
        
        for table in self.schema.tables:
            # Count queries
            for q, sql in self.QUERY_TEMPLATES["count"]:
                queries.append({
                    "question": q.format(table=table.name),
                    "sql": sql.format(table=table.name),
                    "dialect": self.schema.dialect,
                    "referenced_tables": [table.name]
                })
            
            # Top-k queries
            for q, sql in self.QUERY_TEMPLATES["top_k"]:
                for k in [5, 10, 20, 50]:
                    queries.append({
                        "question": q.format(table=table.name, k=k),
                        "sql": sql.format(table=table.name, k=k),
                        "dialect": self.schema.dialect,
                        "referenced_tables": [table.name]
                    })
            
            # Select all
            for q, sql in self.QUERY_TEMPLATES["select_all"]:
                queries.append({
                    "question": q.format(table=table.name),
                    "sql": sql.format(table=table.name),
                    "dialect": self.schema.dialect,
                    "referenced_tables": [table.name]
                })
            
            # Order by
            col = self._get_column_by_type(table, ['int', 'varchar', 'date'])
            if col:
                for q, sql in self.QUERY_TEMPLATES["order_by"]:
                    queries.append({
                        "question": q.format(table=table.name, column=col['name']),
                        "sql": sql.format(table=table.name, column=col['name']),
                        "dialect": self.schema.dialect,
                        "referenced_tables": [table.name]
                    })
                for q, sql in self.QUERY_TEMPLATES["order_by_desc"]:
                    queries.append({
                        "question": q.format(table=table.name, column=col['name']),
                        "sql": sql.format(table=table.name, column=col['name']),
                        "dialect": self.schema.dialect,
                        "referenced_tables": [table.name]
                    })
            
            # Distinct
            str_col = self._get_column_by_type(table, ['varchar', 'text', 'char'])
            if str_col:
                for q, sql in self.QUERY_TEMPLATES["distinct"]:
                    queries.append({
                        "question": q.format(column=str_col['name']),
                        "sql": sql.format(table=table.name, column=str_col['name']),
                        "dialect": self.schema.dialect,
                        "referenced_tables": [table.name]
                    })
            
            # Aggregation queries
            numeric_cols = self._get_numeric_columns(table)
            for col in numeric_cols[:2]:  # Limit to first 2 numeric columns
                for agg, template_key in [("sum", "sum"), ("avg", "avg"), ("min", "min"), ("max", "max")]:
                    for q, sql in self.QUERY_TEMPLATES[template_key]:
                        queries.append({
                            "question": q.format(column=col['name'], table=table.name),
                            "sql": sql.format(table=table.name, column=col['name']),
                            "dialect": self.schema.dialect,
                            "referenced_tables": [table.name]
                        })
            
            # Group by queries
            str_cols = self._get_string_columns(table)
            for col in str_cols[:2]:  # Limit to first 2 string columns
                for q, sql in self.QUERY_TEMPLATES["group_by"]:
                    queries.append({
                        "question": q.format(column=col['name'], table=table.name),
                        "sql": sql.format(table=table.name, column=col['name']),
                        "dialect": self.schema.dialect,
                        "referenced_tables": [table.name]
                    })
            
            # Filter queries
            for col in table.columns[:3]:  # Limit to first 3 columns
                values = self._get_sample_values(col)
                for value in values[:2]:  # Use first 2 values
                    for q, sql in self.QUERY_TEMPLATES["filter"]:
                        queries.append({
                            "question": q.format(table=table.name, column=col['name'], value=value),
                            "sql": sql.format(table=table.name, column=col['name'], value=value),
                            "dialect": self.schema.dialect,
                            "referenced_tables": [table.name]
                        })
            
            # IS NULL / IS NOT NULL
            for col in table.columns[:2]:
                for q, sql in self.QUERY_TEMPLATES["is_null"]:
                    queries.append({
                        "question": q.format(table=table.name, column=col['name']),
                        "sql": sql.format(table=table.name, column=col['name']),
                        "dialect": self.schema.dialect,
                        "referenced_tables": [table.name]
                    })
                for q, sql in self.QUERY_TEMPLATES["not_null"]:
                    queries.append({
                        "question": q.format(table=table.name, column=col['name']),
                        "sql": sql.format(table=table.name, column=col['name']),
                        "dialect": self.schema.dialect,
                        "referenced_tables": [table.name]
                    })
        
        # Limit per table
        limited_queries = []
        table_counts = {}
        for q in queries:
            table = q['referenced_tables'][0]
            table_counts[table] = table_counts.get(table, 0) + 1
            if table_counts[table] <= max_per_table:
                limited_queries.append(q)
        
        return limited_queries
    
    def generate_join_queries(self) -> List[Dict]:
        """Generate queries involving JOINs based on foreign key relationships."""
        queries = []
        
        # Build foreign key map
        fk_map: Dict[str, List[Dict]] = {}
        for table in self.schema.tables:
            for fk in table.foreign_keys or []:
                if fk['ref_table'] not in fk_map:
                    fk_map[fk['ref_table']] = []
                fk_map[fk['ref_table']].append({
                    'from_table': table.name,
                    'from_column': fk['column'],
                    'to_column': fk['ref_column']
                })
        
        # Generate join queries
        for table in self.schema.tables:
            if table.foreign_keys:
                for fk in table.foreign_keys:
                    ref_table = fk['ref_table']
                    
                    # Simple join
                    q = f"Show all {table.name} with their {ref_table} information"
                    sql = f"""SELECT t.*, r.* 
FROM {table.name} t 
JOIN {ref_table} r ON t.{fk['column']} = r.{fk['ref_column']} 
LIMIT 1000"""
                    queries.append({
                        "question": q,
                        "sql": sql,
                        "dialect": self.schema.dialect,
                        "referenced_tables": [table.name, ref_table]
                    })
                    
                    # Join with count
                    q = f"How many {table.name} per {ref_table}"
                    sql = f"""SELECT r.*, COUNT(t.{fk['column']}) AS {table.name}_count 
FROM {ref_table} r 
LEFT JOIN {table.name} t ON t.{fk['column']} = r.{fk['ref_column']} 
GROUP BY r.{fk['ref_column']}"""
                    queries.append({
                        "question": q,
                        "sql": sql,
                        "dialect": self.schema.dialect,
                        "referenced_tables": [table.name, ref_table]
                    })
        
        return queries
    
    def generate_all_queries(self, include_joins: bool = True) -> List[Dict]:
        """Generate all synthetic queries."""
        queries = self.generate_simple_queries()
        
        if include_joins:
            queries.extend(self.generate_join_queries())
        
        # Add schema context to each query
        schema_summary = self._build_full_schema_summary()
        
        for q in queries:
            q['schema_summary'] = schema_summary
            q['retrieved_schema_snippet'] = self._build_relevant_schema(q['referenced_tables'])
        
        return queries
    
    def _build_full_schema_summary(self) -> str:
        """Build full schema summary."""
        lines = [f"Database: {self.schema.database_name}", ""]
        for table in self.schema.tables:
            lines.append(self._format_schema_summary(table))
            lines.append("")
        return "\n".join(lines)
    
    def _build_relevant_schema(self, table_names: List[str]) -> str:
        """Build schema summary for specific tables."""
        lines = []
        for table in self.schema.tables:
            if table.name in table_names:
                lines.append(self._format_schema_summary(table))
                lines.append("")
        return "\n".join(lines) if lines else "No relevant schema found."


def load_schema_from_file(schema_path: str) -> SchemaCatalog:
    """Load schema catalog from JSON file."""
    with open(schema_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return SchemaCatalog.from_dict(data)


def create_sample_schema() -> SchemaCatalog:
    """Create a sample schema for demonstration."""
    tables = [
        TableInfo(
            name="users",
            columns=[
                {"name": "id", "type": "INTEGER", "description": "Primary key"},
                {"name": "username", "type": "VARCHAR(100)", "description": "User's login name"},
                {"name": "email", "type": "VARCHAR(255)", "description": "User's email address"},
                {"name": "created_at", "type": "TIMESTAMP", "description": "Account creation time"},
                {"name": "is_active", "type": "BOOLEAN", "description": "Account status"}
            ],
            primary_key="id",
            foreign_keys=[],
            description="User accounts table"
        ),
        TableInfo(
            name="orders",
            columns=[
                {"name": "id", "type": "INTEGER", "description": "Primary key"},
                {"name": "user_id", "type": "INTEGER", "description": "Foreign key to users"},
                {"name": "total_amount", "type": "DECIMAL(10,2)", "description": "Order total"},
                {"name": "status", "type": "VARCHAR(50)", "description": "Order status"},
                {"name": "created_at", "type": "TIMESTAMP", "description": "Order creation time"}
            ],
            primary_key="id",
            foreign_keys=[{"column": "user_id", "ref_table": "users", "ref_column": "id"}],
            description="Customer orders"
        ),
        TableInfo(
            name="products",
            columns=[
                {"name": "id", "type": "INTEGER", "description": "Primary key"},
                {"name": "name", "type": "VARCHAR(200)", "description": "Product name"},
                {"name": "price", "type": "DECIMAL(10,2)", "description": "Product price"},
                {"name": "category", "type": "VARCHAR(100)", "description": "Product category"},
                {"name": "stock", "type": "INTEGER", "description": "Available stock"}
            ],
            primary_key="id",
            foreign_keys=[],
            description="Product catalog"
        ),
        TableInfo(
            name="order_items",
            columns=[
                {"name": "id", "type": "INTEGER", "description": "Primary key"},
                {"name": "order_id", "type": "INTEGER", "description": "Foreign key to orders"},
                {"name": "product_id", "type": "INTEGER", "description": "Foreign key to products"},
                {"name": "quantity", "type": "INTEGER", "description": "Item quantity"},
                {"name": "unit_price", "type": "DECIMAL(10,2)", "description": "Price at time of order"}
            ],
            primary_key="id",
            foreign_keys=[
                {"column": "order_id", "ref_table": "orders", "ref_column": "id"},
                {"column": "product_id", "ref_table": "products", "ref_column": "id"}
            ],
            description="Order line items"
        )
    ]
    return SchemaCatalog(
        tables=tables,
        database_name="sample_ecommerce",
        dialect="postgres"
    )


def download_spider_dataset(output_dir: str = "./data") -> List[Dict]:
    """
    Download and process Spider dataset.
    Note: This requires internet access to download the dataset.
    For offline use, include the dataset in the project.
    """
    # Spider dataset would be downloaded here
    # For offline systems, include dataset in project
    print("Note: For fully offline systems, include Spider dataset in project data/")
    return []


def merge_datasets(
    synthetic_queries: List[Dict],
    spider_queries: List[Dict] = None,
    dialect: str = "postgres"
) -> List[Dict]:
    """Merge synthetic and Spider datasets."""
    merged = []
    
    # Add synthetic queries
    for q in synthetic_queries:
        q['source'] = 'synthetic'
        q['dialect'] = dialect
        merged.append(q)
    
    # Add Spider queries (if available)
    if spider_queries:
        for q in spider_queries:
            q['source'] = 'spider'
            q['dialect'] = dialect
            merged.append(q)
    
    return merged


def save_dataset(queries: List[Dict], output_path: str):
    """Save dataset to JSON file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(queries, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(queries)} queries to {output_path}")


def load_dataset(input_path: str) -> List[Dict]:
    """Load dataset from JSON file."""
    with open(input_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def main():
    """Main data preparation pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description="NL2SQL Data Preparation")
    parser.add_argument("--schema", type=str, help="Path to schema_catalog.json")
    parser.add_argument("--output", type=str, default="./data/train_dataset.json", help="Output path")
    parser.add_argument("--dialect", type=str, default="postgres", help="SQL dialect")
    parser.add_argument("--include-spider", action="store_true", help="Include Spider dataset")
    parser.add_argument("--sample", action="store_true", help="Generate sample schema data")
    
    args = parser.parse_args()
    
    # Load or create schema
    if args.sample:
        schema = create_sample_schema()
    elif args.schema:
        schema = load_schema_from_file(args.schema)
    else:
        print("No schema provided, using sample schema")
        schema = create_sample_schema()
    
    # Generate synthetic queries
    generator = SyntheticQueryGenerator(schema)
    synthetic = generator.generate_all_queries(include_joins=True)
    
    # Download Spider if requested
    spider_data = []
    if args.include_spider:
        spider_data = download_spider_dataset()
    
    # Merge datasets
    merged = merge_datasets(synthetic, spider_data, args.dialect)
    
    # Save
    save_dataset(merged, args.output)
    
    print(f"Generated {len(merged)} training examples")
    print(f"Dialect: {args.dialect}")
    
    # Print sample
    if merged:
        print("\nSample entry:")
        print(json.dumps(merged[0], indent=2))


if __name__ == "__main__":
    main()