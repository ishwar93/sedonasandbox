"""
intent_router.py
Intent classification via LangChain-Ollama.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Final

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama

logger = logging.getLogger(__name__)

INTENT_LABELS: Final[set[str]] = {"sql", "doc", "both", "none"}
DEFAULT_OLLAMA_MODEL: Final[str] = "qwen3:8b"

_INTENT_EXTRACT_RE = re.compile(r"\b(sql|doc|both|none)\b", re.IGNORECASE)
# Strip both complete <think>...</think> blocks and unclosed <think> blocks
# (unclosed blocks occur when num_predict cuts off the response mid-think).
_THINK_RE = re.compile(r"<think>.*?(?:</think>|$)", re.DOTALL | re.IGNORECASE)

_INTENT_SYSTEM_PROMPT = """You classify questions as one of:
  sql  --> about data in the relational database (counts, aggregates, filters, trends, named entities)
  doc  --> requires narrative, policy, or unstructured content
  both --> needs both structured data and narrative context
  none --> conversational, greeting, out of scope

Respond with EXACTLY one word: sql | doc | both | none. No explanation."""

_llm = ChatOllama(
    model=os.getenv("OLLAMA_MODEL", DEFAULT_OLLAMA_MODEL),
    temperature=0,
    # qwen3:8b emits a <think> block of up to ~1600 chars before the label.
    # 512 tokens covers all observed think blocks with headroom.
    # num_predict must be large enough to cover the full <think> block
    # plus the single-word answer. qwen3:8b think blocks average ~800 tokens.
    # extra_body={"think": False} is not supported in langchain-ollama 1.1.0
    # so we strip <think> blocks in _extract_intent instead.
    num_predict=1024,
    request_timeout=30.0,
)


def _extract_intent(text: str | None) -> str | None:
    if not text:
        return None
    cleaned = _THINK_RE.sub("", text).strip()
    normalized = cleaned.lower()
    if normalized in INTENT_LABELS:
        return normalized
    match = _INTENT_EXTRACT_RE.search(normalized)
    if not match:
        return None
    return match.group(1).lower()


def _heuristic_intent(question: str) -> str:
    q = question.lower()
    sql_markers = [
        "how many", "count", "average", "top ", "near ", "within ",
        "stops", "alerts", "station", "route", "bus", "subway", "citibike",
        "show me", "list", "find", "where", "which", "what is", "how old",
    ]
    if any(marker in q for marker in sql_markers):
        return "sql"
    if any(marker in q for marker in ["hello", "hi", "thanks", "thank you"]):
        return "none"
    return "none"


def classify_intent(question: str) -> tuple[str, str]:
    """
    Return (intent, source) where source is 'ollama_langchain' or 'heuristic_fallback'.
    """
    try:
        response = _llm.invoke([
            SystemMessage(content=_INTENT_SYSTEM_PROMPT),
            HumanMessage(content=question),
        ])
        intent = _extract_intent(response.content)
        if intent:
            logger.debug(
                "intent_router: ollama classified %r -> %s", question[:60], intent
            )
            return intent, "ollama_langchain"
        logger.warning(
            "intent_router: unparseable response %r for %r, using heuristic",
            response.content[:80], question[:60],
        )
    except Exception as exc:
        logger.warning("intent_router: ollama unavailable (%s), using heuristic", exc)

    fallback = _heuristic_intent(question)
    logger.debug(
        "intent_router: heuristic classified %r -> %s", question[:60], fallback
    )
    return fallback, "heuristic_fallback"
