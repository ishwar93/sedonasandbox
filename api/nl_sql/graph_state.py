# api/nl_sql/graph_state.py
"""
NLSQLState — shared state dict threaded through all LangGraph nodes.

Field ownership:
  question        → caller (make_initial_state)
  linked_tables   → schema_linker_node
  draft_sql       → schema_linker_node (observability)
  focused_schema  → schema_linker_node, from LinkingResult.focused_schema
                    (DDL + ontology + JOIN_PATTERNS + ADMIN_RULES, pre-assembled)
                    Carried through all retries unchanged.
  sql             → sql_generator_node, updated on every retry
  error           → validator_node / executor_node on failure; cleared by sql_generator_node
  rows            → executor_node on success
  attempt         → routing functions, incremented before each retry
  signal          → routing functions / schema_linker_node:
                    None | "signal_1_no_tables" | "signal_2_empty_rows" | "signal_3_data_not_available"
  value_hints     → value_hint_injector_node; [{table, column, value}, ...]
  final_answer    → result_checker_node on success, or Signal-1 early exit
"""
from __future__ import annotations
from typing import Optional, TypedDict


class NLSQLState(TypedDict):
    question:       str
    linked_tables:  list[str]
    draft_sql:      str
    focused_schema: str             # pre-assembled by schema_linker.link()
    sql:            str
    error:          Optional[str]
    rows:           list[dict]
    attempt:        int
    signal:         Optional[str]   # None | "signal_1_no_tables" | "signal_2_empty_rows" | "signal_3_data_not_available"
    value_hints:    list[dict]      # [{table, column, value}, ...]
    final_answer:   Optional[dict]


def make_initial_state(question: str) -> NLSQLState:
    """Return a clean initial state for a new question."""
    return NLSQLState(
        question=question,
        linked_tables=[],
        draft_sql="",
        focused_schema="",
        sql="",
        error=None,
        rows=[],
        attempt=0,
        signal=None,
        value_hints=[],
        final_answer=None,
    )
