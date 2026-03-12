"""
TF-IDF Based Schema Retrieval System

This module implements TF-IDF based retrieval for selecting relevant tables
and columns from a database schema based on user questions.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

logger = logging.getLogger(__name__)


@dataclass
class SchemaItem:
    """A schema item (table or column) with associated metadata."""
    name: str
    item_type: str  # "table" or "column"
    data_type: str = ""
    description: str = ""
    parent_table: str = ""  # For columns
    is_primary_key: bool = False
    is_foreign_key: bool = False
    references: str = ""  # For FK - "table.column"
    
    def to_text(self) -> str:
        """Convert to searchable text."""
        parts = [self.name, self.item_type]
        if self.data_type:
            parts.append(self.data_type)
        if self.description:
            parts.append(self.description)
        if self.is_primary_key:
            parts.append("primary key")
        if self.is_foreign_key:
            parts.append("foreign key")
            if self.references:
                parts.append(f"references {self.references}")
        return " ".join(parts)


@dataclass
class RetrievalResult:
    """Result of schema retrieval."""
    items: List[SchemaItem]
    scores: List[float]
    total_items: int
    
    @property
    def table_names(self) -> List[str]:
        """Get unique table names from retrieval."""
        tables = set()
        for item in self.items:
            if item.item_type == "table":
                tables.add(item.name)
            elif item.parent_table:
                tables.add(item.parent_table)
        return sorted(list(tables))
    
    def get_schema_snippet(self) -> str:
        """Generate schema snippet for LLM."""
        lines = []
        
        # Group by table
        tables: Dict[str, List[SchemaItem]] = defaultdict(list)
        for item in self.items:
            if item.item_type == "table":
                tables[item.name].append(item)
            elif item.parent_table:
                tables[item.parent_table].append(item)
        
        for table_name, items in sorted(tables.items()):
            lines.append(f"Table: {table_name}")
            for item in items:
                if item.item_type == "column":
                    col_line = f"  - {item.name} ({item.data_type})"
                    if item.description:
                        col_line += f": {item.description}"
                    if item.is_primary_key:
                        col_line += " [PK]"
                    if item.is_foreign_key:
                        col_line += f" [FK -> {item.references}]"
                    lines.append(col_line)
        
        return "\n".join(lines)


class SchemaTFIDFRetriever:
    """TF-IDF based schema retrieval system."""
    
    def __init__(
        self,
        max_tables: int = 10,
        max_columns_per_table: int = 20,
        ngram_range: Tuple[int, int] = (1, 2),
        min_df: int = 1
    ):
        self.max_tables = max_tables
        self.max_columns_per_table = max_columns_per_table
        self.ngram_range = ngram_range
        self.min_df = min_df
        
        self.vectorizer: Optional[TfidfVectorizer] = None
        self.schema_items: List[SchemaItem] = []
        self.item_to_idx: Dict[int, int] = {}
        self.table_to_items: Dict[str, List[int]] = {}
        self.is_fitted = False
        
    def _tokenize(self, text: str) -> Set[str]:
        """Tokenize text for matching."""
        # Remove quotes and brackets
        text = text.replace('"', '').replace("'", '').replace('[', '').replace(']', '')
        
        # Split on common delimiters
        tokens = re.split(r'[\W_]+', text.lower())
        
        # Also handle camelCase
        camel_split = re.sub(r'([a-z0-9])([A-Z])', r'\1 \2', text.lower())
        tokens.extend(camel_split.split())
        
        return set(t for t in tokens if t and len(t) > 1)
    
    def fit(self, schema_text: str) -> 'SchemaTFIDFRetriever':
        """
        Build TF-IDF index from schema text.
        
        Args:
            schema_text: Schema text in format:
                Table: table_name
                  - column_name (type): description
        """
        self.schema_items = []
        self.table_to_items = {}
        self.item_to_idx = {}
        
        # Parse schema text
        current_table: Optional[str] = None
        current_columns: List[SchemaItem] = []
        
        lines = schema_text.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Detect table
            if line.lower().startswith('table:'):
                # Save previous table
                if current_table:
                    self._add_table_items(current_table, current_columns)
                
                # Extract table name
                table_name = line.split(':', 1)[1].strip()
                table_name = table_name.strip('"').strip("'").strip('[').strip(']')
                current_table = table_name
                current_columns = []
                
                # Add table as item
                item = SchemaItem(
                    name=table_name,
                    item_type="table",
                    description=line
                )
                self._add_item(item)
                
            # Detect column
            elif line.startswith('-') or line.startswith('  -'):
                col_text = line.lstrip('-').strip()
                
                # Parse column info: name (type): description
                col_name = ""
                col_type = ""
                col_desc = ""
                
                # Extract name and type
                if '(' in col_text:
                    parts = col_text.split('(', 1)
                    col_name = parts[0].strip()
                    type_and_desc = parts[1].rstrip(')')
                    if ':' in type_and_desc:
                        type_part, desc_part = type_and_desc.split(':', 1)
                        col_type = type_part.strip()
                        col_desc = desc_part.strip()
                    else:
                        col_type = type_and_desc.strip()
                else:
                    # No type, just name: description
                    if ':' in col_text:
                        col_name, col_desc = col_text.split(':', 1)
                        col_name = col_name.strip()
                        col_desc = col_desc.strip()
                    else:
                        col_name = col_text.strip()
                
                # Clean column name
                col_name = col_name.strip('"').strip("'").strip('[').strip(']')
                
                if col_name and current_table:
                    item = SchemaItem(
                        name=col_name,
                        item_type="column",
                        data_type=col_type,
                        description=col_desc,
                        parent_table=current_table
                    )
                    self._add_item(item)
                    current_columns.append(item)
        
        # Save last table
        if current_table:
            self._add_table_items(current_table, current_columns)
        
        # Build TF-IDF vectorizer
        if self.schema_items:
            texts = [item.to_text() for item in self.schema_items]
            
            self.vectorizer = TfidfVectorizer(
                ngram_range=self.ngram_range,
                min_df=self.min_df,
                lowercase=True,
                token_pattern=r'(?u)\b\w+\b'
            )
            
            self.tfidf_matrix = self.vectorizer.fit_transform(texts)
            self.is_fitted = True
            
            logger.info(f"TF-IDF index built with {len(self.schema_items)} items")
        else:
            logger.warning("No schema items extracted from text")
        
        return self
    
    def _add_item(self, item: SchemaItem) -> None:
        """Add an item to the index."""
        idx = len(self.schema_items)
        self.schema_items.append(item)
        self.item_to_idx[id(item)] = idx
    
    def _add_table_items(self, table_name: str, columns: List[SchemaItem]) -> None:
        """Add columns for a table."""
        self.table_to_items[table_name] = [self.item_to_idx[id(c)] for c in columns]
    
    def retrieve(
        self,
        query: str,
        top_k: int = 5
    ) -> RetrievalResult:
        """
        Retrieve relevant schema items for a query.
        
        Args:
            query: User question
            top_k: Number of top tables to retrieve
            
        Returns:
            RetrievalResult with relevant schema items
        """
        if not self.is_fitted:
            logger.warning("TF-IDF not fitted, returning empty result")
            return RetrievalResult(items=[], scores=[], total_items=0)
        
        # Transform query
        query_vec = self.vectorizer.transform([query])
        
        # Calculate similarities
        similarities = cosine_similarity(query_vec, self.tfidf_matrix)[0]
        
        # Get top indices
        top_indices = np.argsort(similarities)[::-1]
        
        # Collect top tables
        selected_tables: Set[str] = set()
        selected_items: List[SchemaItem] = []
        selected_scores: List[float] = []
        
        for idx in top_indices:
            item = self.schema_items[idx]
            
            if item.item_type == "table":
                if len(selected_tables) >= top_k:
                    continue
                
                # Add table
                selected_tables.add(item.name)
                selected_items.append(item)
                selected_scores.append(float(similarities[idx]))
                
                # Add columns from this table
                if item.name in self.table_to_items:
                    col_indices = self.table_to_items[item.name][:self.max_columns_per_table]
                    for col_idx in col_indices:
                        col = self.schema_items[col_idx]
                        if col not in selected_items:
                            selected_items.append(col)
                            selected_scores.append(float(similarities[col_idx]))
                            
            elif item.parent_table and item.parent_table not in selected_tables:
                # Skip columns from tables not in top-k
                continue
        
        return RetrievalResult(
            items=selected_items,
            scores=selected_scores,
            total_items=len(self.schema_items)
        )
    
    def save_index(self, path: str) -> None:
        """Save the index to a file."""
        data = {
            'schema_items': [
                {
                    'name': item.name,
                    'item_type': item.item_type,
                    'data_type': item.data_type,
                    'description': item.description,
                    'parent_table': item.parent_table,
                    'is_primary_key': item.is_primary_key,
                    'is_foreign_key': item.is_foreign_key,
                    'references': item.references
                }
                for item in self.schema_items
            ],
            'table_to_items': {
                table: list(indices)
                for table, indices in self.table_to_items.items()
            },
            'max_tables': self.max_tables,
            'max_columns_per_table': self.max_columns_per_table
        }
        
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        
        # Save vectorizer
        import pickle
        vectorizer_path = path.replace('.json', '_vectorizer.pkl')
        with open(vectorizer_path, 'wb') as f:
            pickle.dump(self.vectorizer, f)
            
        logger.info(f"Index saved to {path}")
    
    def load_index(self, path: str) -> 'SchemaTFIDFRetriever':
        """Load the index from a file."""
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Restore items
        self.schema_items = [
            SchemaItem(**item_data)
            for item_data in data['schema_items']
        ]
        
        self.item_to_idx = {id(item): idx for idx, item in enumerate(self.schema_items)}
        self.table_to_items = data.get('table_to_items', {})
        
        self.max_tables = data.get('max_tables', 10)
        self.max_columns_per_table = data.get('max_columns_per_table', 20)
        
        # Load vectorizer
        import pickle
        vectorizer_path = path.replace('.json', '_vectorizer.pkl')
        with open(vectorizer_path, 'rb') as f:
            self.vectorizer = pickle.load(f)
        
        # Rebuild TF-IDF matrix
        texts = [item.to_text() for item in self.schema_items]
        self.tfidf_matrix = self.vectorizer.transform(texts)
        self.is_fitted = True
        
        logger.info(f"Index loaded from {path}")
        return self


# Global instance for caching
_global_retriever: Optional[SchemaTFIDFRetriever] = None
_last_schema_text: str = ""


def get_schema_retriever(schema_text: str, force_rebuild: bool = False) -> SchemaTFIDFRetriever:
    """
    Get or build the TF-IDF schema retriever.
    
    Uses caching to avoid rebuilding for the same schema.
    """
    global _global_retriever, _last_schema_text
    
    if force_rebuild or _global_retriever is None or schema_text != _last_schema_text:
        logger.info("Building new TF-IDF schema index")
        _global_retriever = SchemaTFIDFRetriever().fit(schema_text)
        _last_schema_text = schema_text
        
    return _global_retriever


def retrieve_schema(
    question: str,
    schema_text: str,
    top_k: int = 5
) -> Tuple[str, List[str]]:
    """
    Convenience function to retrieve schema for a question.
    
    Returns:
        Tuple of (schema_snippet, list of table names)
    """
    retriever = get_schema_retriever(schema_text)
    result = retriever.retrieve(question, top_k=top_k)
    
    return result.get_schema_snippet(), result.table_names


# Parse schema from database session functions
def build_schema_text_from_catalog(
    tables: List[Dict],
    foreign_keys: List[Dict] = None
) -> str:
    """
    Build schema text from schema catalog data.
    
    Args:
        tables: List of table definitions
        foreign_keys: List of foreign key definitions
        
    Returns:
        Formatted schema text
    """
    lines = []
    fk_map: Dict[str, List[str]] = defaultdict(list)
    
    # Build FK map
    if foreign_keys:
        for fk in foreign_keys:
            parent_table = fk.get('parent_table', '')
            ref_table = fk.get('ref_table', '')
            ref_column = fk.get('ref_column', '')
            if parent_table and ref_table:
                fk_map[parent_table].append(f"  - {ref_column} -> {ref_table}.{ref_column}")
    
    for table in tables:
        table_name = table.get('name', 'unknown')
        columns = table.get('columns', [])
        
        lines.append(f"Table: {table_name}")
        
        for col in columns:
            col_name = col.get('name', '')
            col_type = col.get('type', '')
            col_desc = col.get('description', '')
            
            col_line = f"  - {col_name} ({col_type})"
            if col_desc:
                col_line += f": {col_desc}"
            
            lines.append(col_line)
        
        # Add FK info
        if table_name in fk_map:
            for fk_line in fk_map[table_name]:
                lines.append(fk_line)
        
        lines.append("")
    
    return "\n".join(lines)