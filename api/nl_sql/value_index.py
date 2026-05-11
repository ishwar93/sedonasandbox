# api/nl_sql/value_index.py
"""
Value retrieval for WHERE-clause literal grounding.

Two mechanisms, one class:

1. LSH (pre-generation, built at startup)
   - Samples up to N_SAMPLE distinct values per TEXT column at startup
   - Builds MinHashLSH index keyed by character trigrams
   - At query time: given string literals extracted from the NL question,
     returns DB values that are similar (catches typos, abbreviations)
   - Used by schema_linker_node to inject value hints BEFORE SQL generation

2. rapidfuzz sampler (Signal-2, on-demand)
   - Called ONLY when executor returns empty rows (Signal 2)
   - Samples up to 30 values from the linked tables' TEXT columns at that moment
   - Fuzzy-matches with partial_ratio against question keywords
   - Returns column-grounded value hints for the retry prompt
   - Handles the Fresno/wrong-column problem and long compound stop names

WHY TWO MECHANISMS:
   LSH is a pre-built index covering the full value space. It's fast at query time
   but requires startup cost and only covers sampled values.
   rapidfuzz sampler is on-demand — no startup cost, but adds a DB round-trip
   on the retry path. It handles values missed by the LSH sample and resolves
   the column-selection ambiguity (which column contains this value?).

REFERENCES:
   AT&T paper (arXiv:2505.19988): LSH on shingles via datasketch, N=10000 for BIRD.
   CHASE-SQL (arXiv:2410.01943): LSH for value retrieval, re-ranked by edit distance.
   Both papers: LSH/fuzzy matching scoped to WHERE-clause literal columns only.
"""
from __future__ import annotations

import logging
import re
from typing import NamedTuple

from datasketch import MinHash, MinHashLSH
from rapidfuzz import fuzz as _fuzz

from api.db import query as db_query

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

_NUM_PERM          = 64     # MinHash permutations — higher = more accurate, more memory
_LSH_THRESHOLD     = 0.3   # Jaccard threshold for LSH retrieval (raised from 0.2 to reduce false positives)
_N_SAMPLE          = 5000  # distinct values sampled per column at build time
_N_SAMPLE_SIGNAL2  = 30    # distinct values sampled per column at Signal-2 time
_PARTIAL_THRESHOLD = 60    # rapidfuzz partial_ratio threshold (0-100)
_MAX_HINTS         = 8     # cap on value hints returned to the prompt

# ── Stopwords for keyword extraction ─────────────────────────────────────────

_STOPWORDS = frozenset({
    "the", "are", "for", "how", "many", "what", "show", "find", "list", "get",
    "give", "all", "any", "now", "that", "this", "with", "from", "where", "have",
    "been", "does", "not", "its", "there", "which", "near", "around", "about",
    "between", "and", "or", "in", "on", "at", "to", "of", "a", "an", "is", "it",
    "do", "can", "by", "up", "out", "current", "currently", "latest", "active",
    "right", "now", "today", "still", "has", "let", "me", "see", "please",
})

# ── LITERAL_COLUMNS registry ──────────────────────────────────────────────────
# Only TEXT columns that appear in WHERE clause filters in real queries.
# Deliberately excludes: PKs, FKs, UUIDs, timestamps, numerics, booleans,
# geometry, and any column where fuzzy matching adds no value.

LITERAL_COLUMNS: list[dict] = [
    # ── apitable_combined_locations ───────────────────────────────────────────
    # location_name has 12541 distinct values total (bus stops dominate).
    # Index subway + citibike separately to ensure they're covered.
    # Bus stops are too numerous (9806) and rarely need exact literal matching.
    {"table": "transit.apitable_combined_locations", "column": "location_name",
     "where": "location_type = 'subway'"},
    {"table": "transit.apitable_combined_locations", "column": "location_name",
     "where": "location_type = 'citibike'"},
    {"table": "transit.apitable_combined_locations", "column": "location_type"},
    # ── gtfs_routes ──────────────────────────────────────────────────────────
    {"table": "transit.gtfs_routes",                 "column": "route_short_name"},
    # ── gtfs_stops ───────────────────────────────────────────────────────────
    {"table": "transit.gtfs_stops",                  "column": "stop_name"},
    # ── service_alerts ───────────────────────────────────────────────────────
    {"table": "transit.service_alerts",              "column": "mercury_alert_type"},
    {"table": "transit.service_alerts",              "column": "feed_source"},
    # ── citibike_stations ─────────────────────────────────────────────────────
    {"table": "transit.citibike_stations",           "column": "station_name"},
    # ── geo_boundaries ───────────────────────────────────────────────────────
    # TODO: uncomment once geo_boundaries table is ingested
    # {"table": "transit.geo_boundaries",              "column": "boundary_name"},
    # {"table": "transit.geo_boundaries",              "column": "borough_name"},
    # ── geo_aliases ──────────────────────────────────────────────────────────
    # TODO: uncomment once geo_aliases table is ingested
    # {"table": "transit.geo_aliases",                 "column": "alias_text"},
    # {"table": "transit.geo_aliases",                 "column": "boundary_name"},
    # ── osm_business ─────────────────────────────────────────────────────────
    {"table": "transit.osm_business",                "column": "osm_value"},
    {"table": "transit.osm_business",                "column": "name"},
    # ── bus/subway positions ──────────────────────────────────────────────────
    {"table": "transit.bus_positions",               "column": "line_name"},
    {"table": "transit.subway_positions",            "column": "route_id"},
    # ── traffic_speeds ───────────────────────────────────────────────────────
    
    {"table": "transit.traffic_speeds",              "column": "borough"},
    {"table": "transit.traffic_speeds",              "column": "data_as_of"},
]

# Group by table for efficient Signal-2 sampling (deduplicate column names)
_COLS_BY_TABLE: dict[str, list[str]] = {}
for _entry in LITERAL_COLUMNS:
    cols = _COLS_BY_TABLE.setdefault(_entry["table"], [])
    if _entry["column"] not in cols:
        cols.append(_entry["column"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _trigrams(s: str) -> set[str]:
    """Character trigrams for a string. Short strings return the string itself."""
    s = s.lower()
    if len(s) < 3:
        return {s}
    return {s[i:i+3] for i in range(len(s) - 2)}


def _make_minhash(value: str) -> MinHash:
    """Build a MinHash signature from character trigrams of value."""
    m = MinHash(num_perm=_NUM_PERM)
    for tg in _trigrams(value):
        m.update(tg.encode("utf-8"))
    return m


def _matches_keyword(keyword: str, value: str) -> bool:
    """
    rapidfuzz partial_ratio: finds the best matching substring window.
    "atlantic" scores 100 against "Atlantic Av-Barclays Ctr" because
    partial_ratio slides the shorter string across the longer one.
    Jaccard/trigram Jaccard would score this ~0.18 (too low).
    """
    return _fuzz.partial_ratio(keyword.lower(), value.lower()) >= _PARTIAL_THRESHOLD


def _extract_keywords(question: str) -> list[str]:
    """Extract content words (3+ chars, not stopwords) from question."""
    return [
        w for w in re.findall(r"\b\w{3,}\b", question.lower())
        if w not in _STOPWORDS
    ]


def _extract_literals_from_sql(sql: str) -> list[str]:
    """
    Extract single-quoted string literals from a SQL statement.
    These are the candidate WHERE-clause values to look up in LSH.
    Example: WHERE location_name = 'Atlantic Av' → ['Atlantic Av']
    """
    raw = re.findall(r"'([^']{2,})'", sql)
    cleaned = []
    for lit in raw:
        stripped = lit.strip('%').strip('_').strip()
        if len(stripped) >= 2:
            cleaned.append(stripped)
    return cleaned


# ── ValueIndex ────────────────────────────────────────────────────────────────

class ValueIndex:
    """
    Manages LSH index (pre-generation) and rapidfuzz sampler (Signal-2).
    Instantiated once as a module-level singleton in this file.
    """

    def __init__(self) -> None:
        self._lsh: MinHashLSH | None = None
        self._store: dict[str, dict] = {}   # lsh_key → {table, column, value}
        self.built: bool = False
        self.size: int = 0

    def build(self, tables: list[str] | None = None) -> None:
        """
        Build the LSH index from Databricks.

        Called once at server startup via api/main.py.
        Samples up to _N_SAMPLE distinct values per column.

        Args:
            tables: if provided, only index these tables (used in tests).
                    If None, indexes all tables in LITERAL_COLUMNS.
        """
        target_cols = [
            e for e in LITERAL_COLUMNS
            if tables is None or e["table"] in tables
        ]

        lsh = MinHashLSH(threshold=_LSH_THRESHOLD, num_perm=_NUM_PERM)
        store: dict[str, dict] = {}
        total = 0

        for entry in target_cols:
            table  = entry["table"]
            column = entry["column"]
            try:
                where_clause = entry.get("where", "")
                where_sql = f"WHERE {where_clause} AND {column} IS NOT NULL" if where_clause else f"WHERE {column} IS NOT NULL"
                rows = db_query(
                    f"SELECT DISTINCT {column} FROM {table} "
                    f"{where_sql} LIMIT {_N_SAMPLE}"
                )
            except Exception as exc:
                logger.warning("value_index.build: sample failed %s.%s: %s", table, column, exc)
                continue

            for row in rows:
                value = list(row.values())[0]
                if not isinstance(value, str) or len(value) < 2:
                    continue
                key = f"{table}|{column}|{value}"
                if key in store:
                    continue
                try:
                    lsh.insert(key, _make_minhash(value))
                    store[key] = {"table": table, "column": column, "value": value}
                    total += 1
                except ValueError:
                    pass   # duplicate key — datasketch raises on duplicate insert

        self._lsh   = lsh
        self._store = store
        self.built  = True
        self.size   = total
        logger.info("value_index: built LSH index with %d values across %d columns", total, len(target_cols))

    def query_lsh(self, literals: list[str]) -> list[dict]:
        """
        Pre-generation: find DB values similar to WHERE-clause string literals.

        Called by schema_linker_node after draft SQL is generated.
        Literals are the single-quoted strings extracted from the draft SQL.

        Returns: list of {table, column, value} dicts, deduplicated, capped at _MAX_HINTS.
        Returns [] immediately if index not built or no literals given.
        """
        if not self.built or not self._lsh or not literals:
            return []

        seen: set[str] = set()
        results: list[dict] = []

        for literal in literals:
            if not literal or len(literal) < 2:
                continue
            try:
                candidates = self._lsh.query(_make_minhash(literal))
            except Exception as exc:
                logger.debug("value_index.query_lsh: query failed for %r: %s", literal, exc)
                continue

            for key in candidates:
                if key not in seen and key in self._store:
                    seen.add(key)
                    results.append(self._store[key])
                    if len(results) >= _MAX_HINTS:
                        return results

        return results

    def sample_and_match(
        self,
        question: str,
        linked_tables: list[str],
    ) -> list[dict]:
        """
        Signal-2 retry: sample TEXT column values from Databricks and
        fuzzy-match against question keywords using rapidfuzz partial_ratio.

        Called by value_hint_injector_node ONLY when executor returns empty rows.
        Adds a DB round-trip but only on the retry path.

        Returns: list of {table, column, value} dicts, capped at _MAX_HINTS.
        """
        keywords = _extract_keywords(question)
        if not keywords:
            return []

        seen: set[str] = set()
        matched: list[dict] = []

        for table in linked_tables:
            cols = _COLS_BY_TABLE.get(table, [])
            for col in cols:
                try:
                    rows = db_query(
                        f"SELECT DISTINCT {col} FROM {table} "
                        f"WHERE {col} IS NOT NULL LIMIT {_N_SAMPLE_SIGNAL2}"
                    )
                except Exception as exc:
                    logger.debug("value_index.sample_and_match: sample failed %s.%s: %s", table, col, exc)
                    continue

                for row in rows:
                    value = list(row.values())[0]
                    if not isinstance(value, str) or len(value) < 2:
                        continue
                    key = f"{table}|{col}|{value}"
                    if key in seen:
                        continue
                    for kw in keywords:
                        if _matches_keyword(kw, value):
                            seen.add(key)
                            matched.append({"table": table, "column": col, "value": value})
                            break
                    if len(matched) >= _MAX_HINTS:
                        return matched

        logger.info(
            "value_index.sample_and_match: question=%r matched=%d",
            question[:60], len(matched),
        )
        return matched


# ── Module-level singleton ────────────────────────────────────────────────────

_index = ValueIndex()


def build_value_index() -> None:
    """
    Called once at server startup (from api/main.py on_startup).
    Builds the LSH index from Databricks. Non-fatal on error — the system
    degrades gracefully (LSH returns no hints, Signal-2 sampler still works).
    """
    try:
        _index.build()
    except Exception as exc:
        logger.error("value_index: startup build failed (degraded mode): %s", exc)


def query_lsh(literals: list[str]) -> list[dict]:
    """Query the module-level LSH index for pre-generation value hints."""
    return _index.query_lsh(literals)


def sample_and_match(question: str, linked_tables: list[str]) -> list[dict]:
    """Signal-2 sampler: rapidfuzz match on demand against linked tables."""
    return _index.sample_and_match(question, linked_tables)


def extract_literals_from_sql(sql: str) -> list[str]:
    """Public wrapper: extract WHERE-clause string literals from SQL."""
    return _extract_literals_from_sql(sql)