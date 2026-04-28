"""
intent_router.py
L3 intent classification via Ollama.
"""

from __future__ import annotations

import os
import re
from typing import Final

import requests


INTENT_LABELS: Final[set[str]] = {"sql", "doc", "both", "none"}
DEFAULT_OLLAMA_BASE_URL: Final[str] = "http://localhost:11434"
DEFAULT_OLLAMA_MODEL: Final[str] = "qwen3-coder:7b"
DEFAULT_TIMEOUT_SECONDS: Final[float] = 8.0

_INTENT_EXTRACT_RE = re.compile(r"\b(sql|doc|both|none)\b", re.IGNORECASE)

_INTENT_SYSTEM_PROMPT = """You classify questions as one of:
  sql  --> about data in the relational database (counts, aggregates, filters, trends, named entities)
  doc  --> requires narrative, policy, or unstructured content
  both --> needs both structured data and narrative context
  none --> conversational, greeting, out of scope

Respond with EXACTLY one word: sql | doc | both | none. No explanation."""


def _build_prompt(question: str) -> str:
    return f"SYSTEM:\n{_INTENT_SYSTEM_PROMPT}\n\nUSER:\n{question}\n"


def _extract_intent(text: str | None) -> str | None:
    if not text:
        return None
    normalized = str(text).strip().lower()
    if normalized in INTENT_LABELS:
        return normalized
    match = _INTENT_EXTRACT_RE.search(normalized)
    if not match:
        return None
    return match.group(1).lower()


def _heuristic_intent(question: str) -> str:
    q = question.lower()
    sql_markers = [
        "how many",
        "count",
        "average",
        "top ",
        "near ",
        "within ",
        "stops",
        "alerts",
        "station",
        "route",
        "bus",
        "subway",
        "citibike",
    ]
    if any(marker in q for marker in sql_markers):
        return "sql"
    if any(marker in q for marker in ["hello", "hi", "thanks", "thank you"]):
        return "none"
    return "none"


def classify_intent(question: str) -> tuple[str, str]:
    """
    Return (intent, source) where source is 'ollama' or 'heuristic_fallback'.
    """
    base_url = os.getenv("OLLAMA_BASE_URL", DEFAULT_OLLAMA_BASE_URL).rstrip("/")
    model = os.getenv("OLLAMA_MODEL", DEFAULT_OLLAMA_MODEL)
    timeout_seconds = float(os.getenv("OLLAMA_TIMEOUT_SECONDS", str(DEFAULT_TIMEOUT_SECONDS)))

    payload = {
        "model": model,
        "prompt": _build_prompt(question),
        "stream": False,
        "options": {
            "temperature": 0,
            "num_predict": 4,
        },
    }

    try:
        resp = requests.post(
            f"{base_url}/api/generate",
            json=payload,
            timeout=timeout_seconds,
        )
        resp.raise_for_status()
        data = resp.json()
        intent = _extract_intent(data.get("response"))
        if intent:
            return intent, "ollama"
    except (requests.RequestException, ValueError, TypeError):
        pass

    return _heuristic_intent(question), "heuristic_fallback"
