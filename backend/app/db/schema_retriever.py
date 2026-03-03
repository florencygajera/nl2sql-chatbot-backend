"""Schema Retriever — pick only the most relevant tables/columns for a question.

Upgrades in this version
------------------------
✅ Better tokenization (snake/camel/dotted/brackets)
✅ Synonym expansion (domain-friendly)
✅ Column-pattern boosts (Id/Date/No/Status/Amount)
✅ Smarter fallback (closest-match instead of first-N tables)

No external deps (Windows-friendly).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable


_WORD_RE = re.compile(r"[A-Za-z0-9]+")


# ---- Domain synonyms (extend as you wish) -----------------------------------
# Keep these short and practical. These help table selection massively.
_SYNONYMS: dict[str, set[str]] = {
    # common business terms
    "invoice": {"bill", "inv", "invoiceno"},
    "receipt": {"payment", "pay", "paid"},
    "customer": {"client", "party", "buyer"},
    "vehicle": {"car", "auto", "veh"},
    "tax": {"gst", "vat"},
    "cheque": {"check", "chq"},
    "bank": {"acct", "account"},
    "status": {"state"},
    "amount": {"amt", "total", "sum"},
    "date": {"dt", "datetime", "day"},
    # app-specific-ish (you can add your own)
    "rojmel": {"rojmeltype", "rojmel_type"},
    "rctax": {"rc_tax", "rc", "rctaxmaster"},
}


def _expand_synonyms(tokens: set[str]) -> set[str]:
    out = set(tokens)
    for t in list(tokens):
        if t in _SYNONYMS:
            out.update(_SYNONYMS[t])
        # reverse mapping: if token appears inside a synonym list, add the key
        for k, vals in _SYNONYMS.items():
            if t in vals:
                out.add(k)
    return out


def _split_identifier(name: str) -> list[str]:
    """Split snake_case / camelCase / dotted identifiers into lower-case tokens."""
    if not name:
        return []

    # Remove quotes/brackets that can appear in schema text
    name = name.replace('"', "").replace("'", "").replace("[", "").replace("]", "")

    # Split on dot/schema and underscores and non-word
    parts = re.split(r"[\W_]+", name)
    tokens: list[str] = []

    for p in parts:
        if not p:
            continue
        # split camelCase
        camel = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", p)
        tokens.extend([t.lower() for t in camel.split() if t])

    return tokens


def _tokenize(text: str) -> set[str]:
    return {w.lower() for w in _WORD_RE.findall(text or "")}


def _normalize_table_name(raw: str) -> str:
    """Normalize table name strings from schema like 'dbo.Table' or '"dbo.Table"'."""
    s = (raw or "").strip().strip('"').strip("'")
    s = s.replace("[", "").replace("]", "")
    return s


def _table_short_name(full_name: str) -> str:
    """dbo.Table -> Table"""
    s = _normalize_table_name(full_name)
    if "." in s:
        return s.split(".", 1)[1]
    return s


def _boost_for_column_patterns(question_tokens: set[str], table_tokens: set[str]) -> float:
    """
    Boost tables that have columns matching common query intents.
    This helps with questions like:
      - "total amount by status"
      - "between dates"
      - "invoice no"
    """
    boost = 0.0

    # If user asks about time/date, prefer tables with date/datetime tokens
    if any(t in question_tokens for t in ("date", "day", "month", "year", "between", "from", "to", "since", "before", "after")):
        if any(t in table_tokens for t in ("date", "datetime", "created", "updated", "dt")):
            boost += 0.08

    # If user asks amount/total/sum, prefer tables with amount/balance/total
    if any(t in question_tokens for t in ("amount", "total", "sum", "amt", "price", "fee")):
        if any(t in table_tokens for t in ("amount", "total", "amt", "price", "tax")):
            boost += 0.08

    # If user asks status, prefer tables with status/state
    if any(t in question_tokens for t in ("status", "state")):
        if "status" in table_tokens or "state" in table_tokens:
            boost += 0.07

    # If user asks for id/details, prefer tables with id tokens
    if any(t in question_tokens for t in ("id", "details", "record", "master")):
        if "id" in table_tokens:
            boost += 0.04

    return boost


@dataclass(frozen=True)
class TableInfo:
    name: str  # may be schema.table for MSSQL
    columns: tuple[str, ...]

    @property
    def tokens(self) -> set[str]:
        t: set[str] = set(_split_identifier(self.name))
        # add short name tokens too (dbo.Table -> table)
        t.update(_split_identifier(_table_short_name(self.name)))
        for c in self.columns:
            t.update(_split_identifier(c))
        return t


def parse_schema_summary(schema_text: str) -> list[TableInfo]:
    """Parse schema text format:
    Table: "dbo.Table"
      - "Column" (type)
    """
    tables: list[TableInfo] = []
    current_name: str | None = None
    cols: list[str] = []

    for line in (schema_text or "").splitlines():
        line = line.strip()
        if not line:
            continue

        if line.startswith("Table:"):
            if current_name:
                tables.append(TableInfo(_normalize_table_name(current_name), tuple(cols)))
            current_name = line.split("Table:", 1)[1].strip()
            current_name = _normalize_table_name(current_name)
            cols = []
            continue

        if line.startswith("  -"):
            raw = line[3:].strip()
            # Example:  - "ColumnName" (int)
            col = ""
            if raw.startswith('"'):
                pieces = raw.split('"')
                if len(pieces) >= 2:
                    col = pieces[1]
            else:
                col = raw.split(" ", 1)[0]
            col = col.strip()
            if col:
                cols.append(col)

    if current_name:
        tables.append(TableInfo(_normalize_table_name(current_name), tuple(cols)))

    return tables


def select_relevant_tables(*, question: str, tables: Iterable[TableInfo], top_k: int = 10) -> list[str]:
    """
    Return table names most relevant to the question.

    Scoring strategy:
    - token overlap between question and (table+columns tokens)
    - synonym-expanded overlap
    - small boosts for patterns like Date/Amount/Status columns
    - fallback: closest partial match on table short name
    """
    q_tokens_raw = _tokenize(question)
    q_tokens = _expand_synonyms(q_tokens_raw)
    tables_list = list(tables)

    scored: list[tuple[float, str]] = []
    for t in tables_list:
        tt = t.tokens
        if not tt:
            continue

        tt_expanded = _expand_synonyms(tt)

        overlap = q_tokens.intersection(tt_expanded)
        if not overlap:
            # no direct overlap — still keep a tiny score if table short name partially matches question tokens
            short = _table_short_name(t.name).lower()
            partial = 1.0 if any(tok in short for tok in q_tokens_raw if len(tok) >= 4) else 0.0
            if partial <= 0:
                continue
            score = 0.02 * partial
            scored.append((score, t.name))
            continue

        # Base score: overlap normalized by question length + regularizer by table token size
        score = (len(overlap) / max(1.0, len(q_tokens))) + (len(overlap) / max(16.0, len(tt_expanded)))

        # Boost based on common column patterns / intent
        score += _boost_for_column_patterns(q_tokens, tt_expanded)

        # Slight preference for "Master" / "Txn" tables when tokens match
        short_tokens = set(_split_identifier(_table_short_name(t.name)))
        if "master" in short_tokens and any(x in q_tokens for x in ("master", "details", "record")):
            score += 0.03

        scored.append((score, t.name))

    scored.sort(key=lambda x: x[0], reverse=True)

    if scored:
        return [name for _, name in scored[:top_k]]

    # Fallback 1: closest-match by short table name vs question text
    q_text = (question or "").lower()
    candidates: list[tuple[int, str]] = []
    for t in tables_list:
        short = _table_short_name(t.name).lower()
        # simple character containment scoring
        s = 0
        if short and short in q_text:
            s += 3
        # token containment
        stoks = set(_split_identifier(short))
        s += len(stoks.intersection(q_tokens_raw))
        if s > 0:
            candidates.append((s, t.name))
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    if candidates:
        return [name for _, name in candidates[:top_k]]

    # Fallback 2: safest non-empty selection (but avoid random first tables)
    # Pick tables that have common business columns
    business_bias = []
    for t in tables_list:
        tt = t.tokens
        bias = 0
        if any(x in tt for x in ("date", "datetime")):
            bias += 1
        if any(x in tt for x in ("amount", "total", "status", "name", "no", "id")):
            bias += 1
        business_bias.append((bias, t.name))
    business_bias.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [name for _, name in business_bias[:top_k]]