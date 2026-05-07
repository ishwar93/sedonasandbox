"""ask.py — NL-SQL endpoint with schema linking observability."""

from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException, Query, Request
from api.nl_sql.agent_v2 import run as nl_sql_run
from api.intent_router import classify_intent
from api.security import SecurityValidationError, enforce_rate_limit, sanitise_question
from api.session import get_or_create_session_id, log_session_query

router = APIRouter(tags=["ask"])
logger = logging.getLogger(__name__)


@router.get("/ask")
def ask(
    request: Request,
    q: str = Query(..., description="Natural-language question."),
    aid: str | None = Query(default=None, description="Optional client answer id."),
):
    """
    NL-SQL pipeline:
      L1 sanitize + rate limit
      L2 session logging
      L3 intent routing via Ollama
      L4 schema linking + SQL generation + Databricks execution
    """
    try:
        enforce_rate_limit(request=request, route_key="ask", limit=12, window_seconds=60)
        question = sanitise_question(q)
        sid = get_or_create_session_id(request)
        answer_id = aid or uuid.uuid4().hex

        logger.info("event=ask_received sid_prefix=%s answer_id=%s", sid[:8], answer_id)
        intent, intent_source = classify_intent(question)
        logger.info(
            "event=intent_routed sid_prefix=%s answer_id=%s intent=%s source=%s",
            sid[:8], answer_id, intent, intent_source,
        )

        log_session_query(session_id=sid, question=question, answer_id=answer_id)

        if intent in ("sql", "both"):
            try:
                result = nl_sql_run(question)

                # Signal-1: question doesn't relate to our transit database
                if result.get("signal") == "signal_1_no_tables":
                    return {
                        "answer_id":     answer_id,
                        "intent":        intent,
                        "intent_source": intent_source,
                        "stage":         "v2_signal1_no_tables",
                        "signal":        "signal_1_no_tables",
                        "clarification": result.get("clarification"),
                        "linked_tables": [],
                    }

                # Signal-3: table structurally empty (ingestion incomplete)
                if result.get("signal") == "signal_3_data_not_available":
                    return {
                        "answer_id":     answer_id,
                        "intent":        intent,
                        "intent_source": intent_source,
                        "stage":         "v2_signal3_data_not_available",
                        "signal":        "signal_3_data_not_available",
                        "clarification": result.get("clarification"),
                        "sql":           result.get("sql", ""),
                        "linked_tables": result.get("linked_tables", []),
                    }

                # Hard fail or max retries exceeded
                if result.get("error") and not result.get("rows"):
                    return {
                        "answer_id":     answer_id,
                        "intent":        intent,
                        "intent_source": intent_source,
                        "stage":         "v2_failed",
                        "signal":        result.get("signal"),
                        "error":         result.get("error"),
                        "sql":           result.get("sql", ""),
                        "attempt":       result.get("attempt", 0),
                        "rows":          [],
                    }

                # Success — rows present, or empty after retry budget exhausted
                return {
                    "answer_id":     answer_id,
                    "intent":        intent,
                    "intent_source": intent_source,
                    "sql":           result["sql"],
                    "rows":          result["rows"],
                    "row_count":     result["row_count"],
                    "linked_tables": result.get("linked_tables", []),
                    "attempt":       result.get("attempt", 0),
                    "signal":        result.get("signal"),
                    "value_hints":   result.get("value_hints", []),
                    "stage":         "v2_langgraph",
                }
            except ValueError as ve:
                logger.warning("nl_sql blocked: %s", ve)
                raise HTTPException(status_code=422, detail=str(ve))
            except Exception as exc:
                logger.exception("nl_sql error for question=%r", question[:60])
                raise HTTPException(status_code=500, detail=str(exc))

        return {
            "answer_id":     answer_id,
            "intent":        intent,
            "intent_source": intent_source,
            "stage":         "v1_non_sql",
            "message":       "This question doesn't appear to be about the NYC transit system. Try asking about buses, subways, or Citi Bike.",
        }

    except SecurityValidationError:
        raise
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
