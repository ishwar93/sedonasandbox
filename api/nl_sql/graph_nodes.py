# api/nl_sql/graph_nodes.py
"""
LangGraph node functions for the NL-SQL state machine (agent_v2).

Node execution order:
  schema_linker_node
    ↓ (signal_1_no_tables → END)
  sql_generator_node
    ↓
  validator_node
    ↓ (syntax error + attempts left → sql_generator_node)
    ↓ (syntax error + no attempts → END)
  executor_node
    ↓ (execution error + attempts left → sql_generator_node)
    ↓ (empty rows + attempts left → value_hint_injector_node → sql_generator_node)
    ↓ (failure + no attempts → END)
  result_checker_node → END
"""
from __future__ import annotations

import logging
import os
import re

import sqlglot

from api.nl_sql.graph_state import NLSQLState
from api.nl_sql.schema_context import FEW_SHOT_EXAMPLES
from api.nl_sql.schema_linker import (
    ADMIN_RULES,
    JOIN_PATTERNS,
    link as schema_link,
)
from api.db import query as db_query

from api.nl_sql.graph_state import NLSQLState
from api.nl_sql.value_index import (
    query_lsh,
    sample_and_match,
    extract_literals_from_sql,
)

from langchain_core.messages import HumanMessage, SystemMessage

logger = logging.getLogger(__name__)

_MAX_RETRIES = 2   # max retries for validator + executor combined

# ── LLM factory (mirrors agent_v1) ──────────────────────────────────────────

def _build_llm():
    provider = os.getenv("NL_SQL_LLM_PROVIDER", "ollama").lower()
    if provider == "ollama":
        from langchain_ollama import ChatOllama
        model = os.getenv("OLLAMA_SQL_MODEL", os.getenv("OLLAMA_MODEL", "qwen3:8b"))
        return ChatOllama(model=model, temperature=0, extra_body={"think": False})
    if provider == "openai":
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"), temperature=0)
    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(model=os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6"), temperature=0)
    raise ValueError(f"Unknown NL_SQL_LLM_PROVIDER: {provider!r}")


_llm = _build_llm()

# ── SQL extraction helpers (mirrors agent_v1) ────────────────────────────────

_THINK_RE  = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
_FENCE_RE  = re.compile(r"```(?:sql)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)
_SELECT_RE = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)


def _extract_sql(raw: str) -> str:
    cleaned = _THINK_RE.sub("", raw).strip()
    m = _FENCE_RE.search(cleaned)
    return m.group(1).strip() if m else cleaned


def _is_select(sql: str) -> bool:
    return bool(_SELECT_RE.match(sql))


# ── Few-shot selector (mirrors agent_v1) ─────────────────────────────────────

def _select_examples(linked_tables: list[str], n: int = 3) -> str:
    if not linked_tables:
        candidates = FEW_SHOT_EXAMPLES[:n]
    else:
        scored = []
        for ex in FEW_SHOT_EXAMPLES:
            score = sum(1 for t in linked_tables if t.split(".")[-1] in ex["sql"].lower())
            scored.append((score, ex))
        scored.sort(key=lambda x: x[0], reverse=True)
        candidates = [ex for _, ex in scored[:n]]
    return "\n\n".join(f"Q: {ex['q']}\nSQL:\n{ex['sql']}" for ex in candidates)


# ── Compressed full schema (mirrors agent_v1._COMPRESSED_FULL_SCHEMA) ─────────

_COMPRESSED_FULL_SCHEMA = """\
transit.apitable_combined_locations: location_id TEXT, location_name TEXT, lat DOUBLE, lon DOUBLE, location_type TEXT ('subway'|'bus'|'citibike'), h3_r8 TEXT, h3_r9 TEXT
transit.gtfs_routes: feed_id TEXT, route_id TEXT, route_short_name TEXT, route_type INT (1=subway,3=bus), route_color TEXT
transit.gtfs_stops: feed_id TEXT, stop_id TEXT, stop_name TEXT, lat DOUBLE, lon DOUBLE
transit.gtfs_trips: feed_id TEXT, trip_id TEXT, route_id TEXT, service_id TEXT, direction_id TEXT
transit.gtfs_calendar: feed_id TEXT, service_id TEXT, monday INT, tuesday INT, wednesday INT, thursday INT, friday INT, saturday INT, sunday INT
transit.gtfs_stop_connections: feed_id TEXT, route_id TEXT, from_stop_id TEXT, to_stop_id TEXT, scheduled_travel_time_sec INT
transit.gtfs_transfers: feed_id TEXT, from_stop_id TEXT, to_stop_id TEXT, transfer_type INT, min_transfer_time INT
transit.service_alerts: alert_id TEXT, feed_source TEXT ('subway'|'bus'), header_text_plain TEXT, mercury_alert_type TEXT, is_planned BOOLEAN, is_active_now BOOLEAN, last_seen_at TIMESTAMP
transit.service_alert_affected_entities: alert_id TEXT, route_id TEXT (nullable), stop_id TEXT (nullable), priority_level INT (1-35)
transit.service_alert_active_periods: alert_id TEXT, starts_at TIMESTAMP, ends_at TIMESTAMP
transit.citibike_stations: station_id TEXT, station_name TEXT, lat DOUBLE, lon DOUBLE, capacity INT
transit.citibike_status: station_id TEXT, bikes_available INT, ebikes_available INT, docks_available INT, is_renting INT, ingested_at TIMESTAMP (APPEND-ONLY: always MAX per station)
transit.geo_boundaries: boundary_type TEXT ('borough'|'neighborhood'), boundary_name TEXT, borough_name TEXT, nta_type TEXT, geom GEOMETRY
transit.geo_aliases: alias_text TEXT, boundary_type TEXT, boundary_name TEXT
transit.subway_positions: trip_id TEXT, route_id TEXT, direction TEXT, location_stop TEXT, location_status TEXT, ingested_at TIMESTAMP
transit.bus_positions: vehicle_ref TEXT, line_name TEXT, lat DOUBLE, lon DOUBLE, passenger_count INT, passenger_capacity INT, ingested_at TIMESTAMP (filter last 2h)
transit.traffic_speeds: segment_id TEXT, link_name TEXT, borough TEXT, speed DOUBLE, ingested_at TIMESTAMP
transit.osm_business: osm_id TEXT, name TEXT, osm_value TEXT, lat DOUBLE, lon DOUBLE, postal_code TEXT
transit.osm_business_hours: osm_id TEXT, day_of_week TEXT, open_time TEXT, close_time TEXT
transit.gtfs_feed_versions: feed_id TEXT, feed_type TEXT, downloaded_at TIMESTAMP"""


def _build_sql_prompt(
    question: str,
    focused_schema: str,
    linked_tables: list[str],
    error: str | None = None,
    previous_sql: str | None = None,
    value_hints: list[dict] | None = None,
) -> str:
    """
    Build the SQL generation prompt.

    On first attempt: question + focused schema + examples.
    On retry: prepend error context and (optionally) value hints.
    value_hints: list of {table, column, value} injected as SQL comments.
    """
    examples = _select_examples(linked_tables)

    if focused_schema:
        schema_block = focused_schema
        schema_note  = f"-- Schema linked to: {', '.join(linked_tables)}"
    else:
        schema_block = f"{_COMPRESSED_FULL_SCHEMA}\n\n{JOIN_PATTERNS}\n\n{ADMIN_RULES}"
        schema_note  = "-- Full schema (schema linking fallback)"

    # Value hints block — injected between schema and examples on retry
    hints_block = ""
    if value_hints:
        hint_lines = "\n".join(
            f"-- '{h['value']}' found in {h['table']}.{h['column']}"
            for h in value_hints
        )
        hints_block = f"\n-- MATCHING DB VALUES (use in WHERE clauses):\n{hint_lines}\n"

    # Error context block — injected on retry
    error_block = ""
    if error and previous_sql:
        error_block = (
            f"\n-- PREVIOUS ATTEMPT FAILED:\n"
            f"-- Error: {error}\n"
            f"-- Previous SQL:\n-- {previous_sql.replace(chr(10), chr(10)+'-- ')}\n"
            f"-- Fix the SQL and try again.\n"
        )

    return (
        f"/nothink\n"
        f"{schema_note}\n"
        f"{schema_block}\n"
        f"{hints_block}"
        f"{error_block}"
        f"\n-- EXAMPLES:\n{examples}\n"
        f"\n-- Generate SQL for: {question}\nSQL:"
    )


# ════════════════════════════════════════════════════════════════════════════
#  NODE 1: schema_linker_node
# ════════════════════════════════════════════════════════════════════════════

def schema_linker_node(state: NLSQLState) -> NLSQLState:
    """
    Step 1: Reversed schema linking.

    Calls schema_linker.link() which:
      - Asks LLM to write a draft SQL (short prompt, ~300 tokens)
      - Parses table names from draft SQL using sqlglot (deterministic)
      - Builds focused schema for those tables only

    Signal 1 detection: if no transit tables are found in the draft SQL,
    the question does not translate to our database. Set signal and
    final_answer immediately — graph will route to END.
    """
    linking = schema_link(state["question"], llm=_llm)
    logger.info(
        "schema_linker_node: linked=%s fallback=%s schema_chars=%d",
        linking.linked_tables, linking.fallback_used, len(linking.focused_schema),
    )

    state["linked_tables"] = linking.linked_tables
    state["draft_sql"]     = linking.draft_sql
    state["focused_schema"] = linking.focused_schema  # pre-assembled: DDL + ontology + JOIN_PATTERNS + ADMIN_RULES

    # ── LSH pre-generation value hints ────────────────────────────────────────
    # Extract string literals from the draft SQL, query LSH index,
    # inject matching DB values into state so sql_generator_node can use them.
    # Only fires when draft SQL contains quoted string literals.
    # Returns [] gracefully if LSH index not yet built.
    draft_literals = extract_literals_from_sql(linking.draft_sql)
    lsh_hints = query_lsh(draft_literals)
    if lsh_hints:
        state["value_hints"] = lsh_hints
        logger.info(
            "schema_linker_node: LSH pre-gen hints=%d for literals=%s",
            len(lsh_hints), draft_literals[:3],
        )

    # ── Signal 1: no tables found ────────────────────────────────────────────
    # The draft SQL couldn't reference any known transit tables.
    # This almost always means the question is not about our database.
    if not linking.linked_tables:
        logger.warning(
            "schema_linker_node: SIGNAL_1 — no tables for question=%r",
            state["question"][:80],
        )
        state["signal"] = "signal_1_no_tables"
        state["final_answer"] = {
            "signal":       "signal_1_no_tables",
            "clarification": (
                "Your question doesn't seem to relate to the NYC transit database. "
                "Try asking about subway stops, bus routes, Citi Bike availability, "
                "service alerts, or businesses near transit stations."
            ),
            "question":     state["question"],
            "linked_tables": [],
        }
    return state

# ════════════════════════════════════════════════════════════════════════════
#  NODE 2: sql_generator_node
# ════════════════════════════════════════════════════════════════════════════

def sql_generator_node(state: NLSQLState) -> NLSQLState:
    """
    Step 2 (and retry): Generate final SQL using focused schema from state.

    state["focused_schema"] is the pre-assembled string from schema_linker.link():
      DDL blocks + ontology comment block + JOIN_PATTERNS + ADMIN_RULES.
    This is reused as-is on every retry — never rebuilt — so the exact same
    schema context the linker prepared is what the generator sees every time.

    On first attempt: clean generation.
    On retry: error context and/or value hints are injected into the prompt
    so the LLM knows what went wrong and what DB values to use.
    """
    prompt = _build_sql_prompt(
        question       = state["question"],
        focused_schema = state["focused_schema"],   # from state — never rebuilt
        linked_tables  = state["linked_tables"],
        error          = state.get("error"),
        previous_sql   = state.get("sql") or None,
        value_hints    = state.get("value_hints") or None,
    )

    response = _llm.invoke([HumanMessage(content=prompt)])
    sql = _extract_sql(response.content)

    logger.info(
        "sql_generator_node: attempt=%d sql=%r",
        state["attempt"], sql[:80],
    )

    state["sql"]   = sql
    state["error"] = None   # clear previous error — validator will re-set if needed
    return state

# ════════════════════════════════════════════════════════════════════════════
#  NODE 3: validator_node
# ════════════════════════════════════════════════════════════════════════════

def validator_node(state: NLSQLState) -> NLSQLState:
    """
    Step 3: Deterministic SQL validation.

    Checks:
      (a) Non-SELECT blocked (safety gate — no retry, hard fail)
      (b) sqlglot parse in Databricks dialect (syntax check — retry eligible)

    Sets state["error"] on failure. Returns state unchanged on success.
    """
    sql = state["sql"]

    # Safety gate: block any non-SELECT before sqlglot even runs
    if not _is_select(sql):
        state["error"] = f"Non-SELECT SQL blocked for safety: {sql[:80]!r}"
        logger.warning("validator_node: non-SELECT blocked: %r", sql[:80])
        return state

    # Syntax check
    try:
        sqlglot.parse_one(sql, dialect="databricks")
    except sqlglot.errors.ParseError as exc:
        state["error"] = f"Syntax error: {exc}"
        logger.warning("validator_node: syntax error: %s", exc)

    return state

# ════════════════════════════════════════════════════════════════════════════
#  NODE 4: executor_node
# ════════════════════════════════════════════════════════════════════════════

def executor_node(state: NLSQLState) -> NLSQLState:
    """
    Step 4: Execute validated SQL against Databricks.

    On DB exception: set error (retry eligible).
    On success: populate rows, clear error.
    Empty rows (Signal 2) are NOT set as an error here — result_checker_node
    handles that distinction so the executor's job stays simple.
    """
    try:
        rows = db_query(state["sql"])
        state["rows"]  = rows
        state["error"] = None
        logger.info(
            "executor_node: success rows=%d sql=%r",
            len(rows), state["sql"][:80],
        )
    except Exception as exc:
        state["error"] = f"Execution error: {exc}"
        state["rows"]  = []
        logger.warning("executor_node: DB error: %s", exc)

    return state


# ════════════════════════════════════════════════════════════════════════════
#  NODE 5: value_hint_injector_node  (Signal-2 retry path only)
# ════════════════════════════════════════════════════════════════════════════

def value_hint_injector_node(state: NLSQLState) -> NLSQLState:
    """
    Signal-2 retry preparation node.

    Fires ONLY when executor returns empty rows and retry budget remains.
    Calls sample_and_match (rapidfuzz partial_ratio) against the linked tables
    to find column-grounded DB values similar to question keywords.
    Sets signal="signal_2_empty_rows" for LangSmith observability.

    The sql_generator_node picks up state["value_hints"] on the next call
    and injects them as SQL comments so the LLM uses the correct literal values.
    """
    hints = sample_and_match(
        question=state["question"],
        linked_tables=state["linked_tables"],
    )
    state["value_hints"] = hints
    state["signal"] = "signal_2_empty_rows"

    logger.info(
        "value_hint_injector_node: signal_2 matched=%d hints for question=%r",
        len(hints), state["question"][:60],
    )
    return state


# ════════════════════════════════════════════════════════════════════════════
#  NODE 6: result_checker_node
# ════════════════════════════════════════════════════════════════════════════

def result_checker_node(state: NLSQLState) -> NLSQLState:
    """
    Terminal success node. Assembles final_answer from executor output.
    Reached on successful execution (rows present, or empty after retry budget
    exhausted — we return empty rather than hide the result from the caller).
    """
    state["final_answer"] = {
        "sql":           state["sql"],
        "rows":          state["rows"],
        "row_count":     len(state["rows"]),
        "linked_tables": state["linked_tables"],
        "attempt":       state["attempt"],
        "signal":        state.get("signal"),
        "value_hints":   state.get("value_hints", []),
    }
    logger.info(
        "result_checker_node: done rows=%d attempt=%d signal=%s",
        len(state["rows"]), state["attempt"], state.get("signal"),
    )
    return state


# ════════════════════════════════════════════════════════════════════════════
#  HELPERS for routing
# ════════════════════════════════════════════════════════════════════════════

# Known live/operational tables that may be empty during early ingestion.
_LIVE_TABLES = frozenset({
    "transit.citibike_status",
    "transit.subway_positions",
    "transit.bus_positions",
    "transit.traffic_speeds",
    "transit.service_alerts",
    "transit.service_alert_affected_entities",
    "transit.service_alert_active_periods",
})


def _tables_are_empty(linked_tables: list[str]) -> bool:
    """
    Check if ANY of the linked tables contains zero rows.

    Called before Signal-2 retry to distinguish:
      - Signal-2: table has data, WHERE clause matched nothing (retry useful)
      - Signal-3: table has no data yet (ingestion incomplete, retry pointless)

    Non-fatal on DB error — returns False so Signal-2 retry is attempted
    rather than falsely reporting Signal-3.
    """
    for table in linked_tables:
        try:
            rows = db_query(f"SELECT COUNT(*) AS n FROM {table}")
            if rows and list(rows[0].values())[0] == 0:
                logger.info("_tables_are_empty: %s has zero rows (ingestion incomplete)", table)
                return True
        except Exception as exc:
            logger.debug("_tables_are_empty: count failed for %s: %s", table, exc)
    return False


# ════════════════════════════════════════════════════════════════════════════
#  ROUTING FUNCTIONS (conditional edges)
# ════════════════════════════════════════════════════════════════════════════

def route_after_schema_linker(state: NLSQLState) -> str:
    """Signal-1 → end. Otherwise → sql_generator."""
    if state.get("signal") == "signal_1_no_tables":
        return "end"
    return "sql_generator"


def route_after_validator(state: NLSQLState) -> str:
    """
    No error → executor.
    Non-SELECT (hard fail, no retry) → end.
    Syntax error + budget → sql_generator (increment attempt).
    Syntax error + no budget → end.
    """
    error = state.get("error")
    if not error:
        return "executor"

    if "Non-SELECT" in error:
        state["final_answer"] = {
            "error":  error,
            "signal": "hard_fail_non_select",
            "sql":    state["sql"],
            "rows":   [],
        }
        logger.warning("route_after_validator: hard fail non-SELECT")
        return "end"

    if state["attempt"] < _MAX_RETRIES:
        state["attempt"] += 1
        logger.info("route_after_validator: syntax retry attempt=%d", state["attempt"])
        return "sql_generator"

    state["final_answer"] = {
        "error":   error,
        "signal":  "max_retries_exceeded",
        "sql":     state["sql"],
        "rows":    [],
        "attempt": state["attempt"],
    }
    return "end"


def route_after_executor(state: NLSQLState) -> str:
    """
    DB error + budget → sql_generator (increment attempt).
    DB error + no budget → end.
    Empty rows + Signal-3 (table structurally empty) → end with clarification.
    Empty rows + budget → value_hint_injector (increment attempt) [Signal-2].
    Empty rows + no budget → result_checker (return empty, don't hide it).
    Rows returned → result_checker.
    """
    error = state.get("error")
    rows  = state.get("rows", [])

    if error:
        if state["attempt"] < _MAX_RETRIES:
            state["attempt"] += 1
            logger.info("route_after_executor: db error retry attempt=%d", state["attempt"])
            return "sql_generator"
        state["final_answer"] = {
            "error":   error,
            "signal":  "max_retries_exceeded",
            "sql":     state["sql"],
            "rows":    [],
            "attempt": state["attempt"],
        }
        return "end"

    if len(rows) == 0:
        # ── Signal-3 check: is the table itself empty? ────────────────────────
        # Must check before Signal-2 retry — retrying with different literal
        # values won't help if the table has no rows due to incomplete ingestion.
        if _tables_are_empty(state["linked_tables"]):
            is_live = any(t in _LIVE_TABLES for t in state["linked_tables"])
            clarification = (
                "This query involves live operational data (real-time feeds) "
                "that hasn't been ingested yet. The polling pipeline may still "
                "be warming up — try again in a few minutes."
                if is_live else
                "The data for this query exists in the database schema but "
                "hasn't been populated yet. Try again after the next ingestion run."
            )
            state["signal"] = "signal_3_data_not_available"
            state["final_answer"] = {
                "signal":        "signal_3_data_not_available",
                "clarification": clarification,
                "sql":           state["sql"],
                "rows":          [],
                "linked_tables": state["linked_tables"],
                "attempt":       state["attempt"],
            }
            logger.warning(
                "route_after_executor: SIGNAL_3 — empty table(s) %s",
                state["linked_tables"],
            )
            return "end"

        # ── Signal-2: table has data, WHERE clause matched nothing ────────────
        if state["attempt"] < _MAX_RETRIES:
            state["attempt"] += 1
            logger.info(
                "route_after_executor: signal_2 empty rows, injecting value hints attempt=%d",
                state["attempt"],
            )
            return "value_hint_injector"

    return "result_checker"