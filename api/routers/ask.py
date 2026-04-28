"""ask.py — L3 intent-router scaffold endpoint."""

from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException, Query, Request

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
    Minimal L3 scaffold:
    - L1 sanitize + rate limit
    - L2 session logging
    - L3 intent routing via Ollama
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
            sid[:8],
            answer_id,
            intent,
            intent_source,
        )

        log_session_query(
            session_id=sid,
            question=question,
            answer_id=answer_id,
        )

        return {
            "answer_id": answer_id,
            "intent": intent,
            "intent_source": intent_source,
            "stage": "L3_scaffold",
            "message": (
                "Intent classified. L4-L10 pipeline is not wired yet; "
                "this endpoint currently validates and routes only."
            ),
        }
    except SecurityValidationError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
