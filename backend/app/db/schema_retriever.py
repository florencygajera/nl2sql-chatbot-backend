"""Schema Retriever — pick only the most relevant tables/columns for a question.

Passing a huge schema into a small local LLM often reduces NL→SQL accuracy.
This module helps by:
- parsing the existing schema summary
- selecting a small set of relevant tables via fast token overlap heuristics

No external deps (keeps Windows installs easy).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable


_WORD_RE = re.compile(r"[A-Za-z0-9]+")


def _split_identifier(name: str) -> list[str]:
    """Split snake_case / camelCase / dotted identifiers into lower-case tokens."""
    if not name:
        return []

    parts = re.split(r"[\W_]+", name)
    tokens: list[str] = []

    for p in parts:
        if not p:
            continue
        camel = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", p)
        tokens.extend([t.lower() for t in camel.split() if t])

    return tokens


def _tokenize(text: str) -> set[str]:
    return {w.lower() for w in _WORD_RE.findall(text or "")}


@dataclass(frozen=True)
class TableInfo:
    name: str  # may be schema.table for MSSQL
    columns: tuple[str, ...]

    @property
    def tokens(self) -> set[str]:
        t: set[str] = set(_split_identifier(self.name))
        for c in self.columns:
            t.update(_split_identifier(c))
        return t


def parse_schema_summary(schema_text: str) -> list[TableInfo]:
    """Parse the schema summary format produced by get_schema_summary()."""
    tables: list[TableInfo] = []
    current_name: str | None = None
    cols: list[str] = []

    for line in (schema_text or "").splitlines():
        line = line.strip()
        if not line:
            continue

        if line.startswith("Table:"):
            if current_name:
                tables.append(TableInfo(current_name, tuple(cols)))
            current_name = line.split("Table:", 1)[1].strip().strip('"')
            cols = []
            continue

        if line.startswith("  -"):
            # Example:  - "ColumnName" (int)
            raw = line[3:].strip()
            if raw.startswith('"'):
                # take quoted name
                pieces = raw.split('"')
                if len(pieces) >= 2:
                    col = pieces[1]
                else:
                    col = raw
            else:
                col = raw.split(" ", 1)[0]
            if col:
                cols.append(col)

    if current_name:
        tables.append(TableInfo(current_name, tuple(cols)))

    return tables


def select_relevant_tables(*, question: str, tables: Iterable[TableInfo], top_k: int = 10) -> list[str]:
    """Return table names most relevant to the question."""
    q_tokens = _tokenize(question)
    tables_list = list(tables)

    scored: list[tuple[float, str]] = []
    for t in tables_list:
        tt = t.tokens
        if not tt:
            continue
        overlap = q_tokens.intersection(tt)
        if not overlap:
            continue

        # Heuristic score: overlap normalized by question length + small regularizer
        score = (len(overlap) / max(1.0, len(q_tokens))) + (len(overlap) / max(12.0, len(tt)))
        scored.append((score, t.name))

    scored.sort(key=lambda x: x[0], reverse=True)

    if not scored:
        # fallback: just pick the first few tables (better than empty)
        return [t.name for t in tables_list[: min(top_k, 10)]]

    return [name for _, name in scored[:top_k]]
