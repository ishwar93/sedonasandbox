# api/nl_sql/agent_v2.py
"""
NL-SQL Agent v2 — LangGraph state machine.

Graph topology:
  schema_linker_node
    ├─ signal_1_no_tables → END       (question not about transit DB)
    └─ ok → sql_generator_node
               ↓
         validator_node
           ├─ non-SELECT (hard fail) → END
           ├─ syntax error + budget → sql_generator_node
           ├─ syntax error + no budget → END
           └─ ok → executor_node
                      ├─ db error + budget → sql_generator_node
                      ├─ db error + no budget → END
                      ├─ signal_3 (table empty, ingestion incomplete) → END
                      ├─ empty rows + budget → value_hint_injector_node
                      │                          (rapidfuzz Signal-2)
                      │                          └─ sql_generator_node
                      └─ rows (or empty, no budget) → result_checker_node → END

LangSmith traces every node as a separate span automatically when
LANGSMITH_TRACING=true and LANGSMITH_API_KEY are set in .env.
"""
from __future__ import annotations

import logging

from langgraph.graph import StateGraph, END

from api.nl_sql.graph_state import NLSQLState, make_initial_state
from api.nl_sql.graph_nodes import (
    schema_linker_node,
    sql_generator_node,
    validator_node,
    executor_node,
    value_hint_injector_node,
    result_checker_node,
    route_after_schema_linker,
    route_after_validator,
    route_after_executor,
)

logger = logging.getLogger(__name__)


def _build_graph():
    g = StateGraph(NLSQLState)

    # ── Nodes ────────────────────────────────────────────────────────────────
    g.add_node("schema_linker",       schema_linker_node)
    g.add_node("sql_generator",       sql_generator_node)
    g.add_node("validator",           validator_node)
    g.add_node("executor",            executor_node)
    g.add_node("value_hint_injector", value_hint_injector_node)
    g.add_node("result_checker",      result_checker_node)

    # ── Entry ─────────────────────────────────────────────────────────────────
    g.set_entry_point("schema_linker")

    # ── Edges ─────────────────────────────────────────────────────────────────
    g.add_conditional_edges(
        "schema_linker",
        route_after_schema_linker,
        {"end": END, "sql_generator": "sql_generator"},
    )
    g.add_edge("sql_generator", "validator")
    g.add_conditional_edges(
        "validator",
        route_after_validator,
        {"executor": "executor", "sql_generator": "sql_generator", "end": END},
    )
    g.add_conditional_edges(
        "executor",
        route_after_executor,
        {
            "result_checker":      "result_checker",
            "sql_generator":       "sql_generator",
            "value_hint_injector": "value_hint_injector",
            "end":                 END,
        },
    )
    g.add_edge("value_hint_injector", "sql_generator")
    g.add_edge("result_checker", END)

    return g.compile()


_graph = _build_graph()


def run(question: str) -> dict:
    """
    Execute the v2 LangGraph state machine for a question.

    Returns a flat dict for JSON serialisation in ask.py:
      Success:       {sql, rows, row_count, linked_tables, attempt, signal, value_hints}
      Signal-1:      {signal: "signal_1_no_tables", clarification: str, linked_tables: []}
      Signal-3:      {signal: "signal_3_data_not_available", clarification: str, ...}
      Hard fail:     {signal: str, error: str, sql: str, rows: [], attempt: int}
    """
    initial = make_initial_state(question)
    final   = _graph.invoke(initial)

    answer = final.get("final_answer")
    if answer is None:
        logger.error("agent_v2.run: final_answer is None — state=%s", final)
        answer = {
            "error":   "Graph completed without setting final_answer",
            "signal":  "internal_error",
            "sql":     final.get("sql", ""),
            "rows":    [],
            "attempt": final.get("attempt", 0),
        }

    return {
        "sql":           answer.get("sql", ""),
        "rows":          answer.get("rows", []),
        "row_count":     answer.get("row_count", len(answer.get("rows", []))),
        "linked_tables": answer.get("linked_tables", []),
        "attempt":       answer.get("attempt", 0),
        "signal":        answer.get("signal"),
        "value_hints":   answer.get("value_hints", []),
        "clarification": answer.get("clarification"),
        "error":         answer.get("error"),
    }
