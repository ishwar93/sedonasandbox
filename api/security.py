"""
security.py
Application-level LLM boundary guards used in place of API gateway policies.
"""

from __future__ import annotations

import re
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from threading import Lock
from typing import Any, Iterable

from fastapi import Request


MAX_QUESTION_CHARS = 1000

# Users should ask in natural language; direct SQL passthrough is blocked.
SQL_PASSTHROUGH_PATTERN = re.compile(
    r"^\s*(SELECT|WITH|INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|MERGE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)

# Lightweight prompt-injection patterns that are generally unsafe for NL-to-SQL entrypoints.
PROMPT_INJECTION_PATTERNS = [
    re.compile(r"\bignore\s+(all\s+)?(previous|prior)\s+instructions?\b", re.IGNORECASE),
    re.compile(r"\breveal\s+(the\s+)?(system|developer)\s+prompt\b", re.IGNORECASE),
    re.compile(r"\bshow\s+(the\s+)?(system|developer)\s+message\b", re.IGNORECASE),
    re.compile(r"\bexecute\s+tool\b", re.IGNORECASE),
    re.compile(r"\bfunction\s+call\b", re.IGNORECASE),
]

# HTML/JS payload fragments should never be echoed as streamed assistant output.
OUTPUT_BLOCK_PATTERNS = [
    re.compile(r"<\s*script\b", re.IGNORECASE),
    re.compile(r"javascript\s*:", re.IGNORECASE),
]

# Block common internal metadata fields that should never be user-visible.
BLOCKED_METADATA_KEY_PATTERNS = [
    re.compile(r"^(raw_)?metadata$", re.IGNORECASE),
    re.compile(r"^(doc|document|chunk|node|embedding|vector)_id$", re.IGNORECASE),
    re.compile(r"^(source|file|storage)_(path|uri|url)$", re.IGNORECASE),
    re.compile(r"^(retrieval|rerank|similarity|embedding)_score$", re.IGNORECASE),
    re.compile(r"^internal(_.*)?$", re.IGNORECASE),
]

# Detect internal filesystem or stack details in generated output text.
SENSITIVE_TEXT_PATTERNS = [
    re.compile(r"[A-Za-z]:\\[^ \n\r\t]+"),  # Windows path
    re.compile(r"/(Users|home|var|etc|opt|srv)/[^ \n\r\t]+"),  # Unix-like path
    re.compile(r"\b(traceback|stack trace|exception in)\b", re.IGNORECASE),
]


class SecurityValidationError(ValueError):
    """Raised when security validation fails."""


def _normalize_whitespace(text: str) -> str:
    text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def sanitise_question(question: str, *, max_chars: int = MAX_QUESTION_CHARS) -> str:
    """
    Validate and normalize a natural-language question before sending it to an LLM.

    Raises:
        SecurityValidationError: if input is empty, too long, SQL passthrough, or likely injection.
    """
    if question is None:
        raise SecurityValidationError("Question is required.")

    cleaned = _normalize_whitespace(str(question))
    if not cleaned:
        raise SecurityValidationError("Question is empty after sanitization.")

    if len(cleaned) > max_chars:
        raise SecurityValidationError(f"Question exceeds {max_chars} characters.")

    if SQL_PASSTHROUGH_PATTERN.search(cleaned):
        raise SecurityValidationError("Raw SQL is not allowed. Ask in natural language.")

    for pattern in PROMPT_INJECTION_PATTERNS:
        if pattern.search(cleaned):
            raise SecurityValidationError("Question contains blocked prompt-injection pattern.")

    return cleaned


def validate_output_token(token: str) -> str:
    """
    Validate streamed output token/chunk before forwarding to clients.

    Returns:
        The original token if valid.

    Raises:
        SecurityValidationError: if token appears to contain unsafe HTML/JS payloads.
    """
    text = str(token)
    for pattern in OUTPUT_BLOCK_PATTERNS:
        if pattern.search(text):
            raise SecurityValidationError("Blocked unsafe output token.")
    for pattern in SENSITIVE_TEXT_PATTERNS:
        if pattern.search(text):
            raise SecurityValidationError("Blocked sensitive output token.")
    return text


def _is_blocked_metadata_key(key: str) -> bool:
    key_text = str(key).strip()
    for pattern in BLOCKED_METADATA_KEY_PATTERNS:
        if pattern.search(key_text):
            return True
    return False


def sanitize_public_payload(payload: Any) -> Any:
    """
    Recursively sanitize payloads before returning user-visible API responses.

    Behavior:
    - Drops blocked metadata/internal keys from dictionaries.
    - Validates strings against output token rules.
    - Preserves shape for lists and primitive values.
    """
    if isinstance(payload, dict):
        cleaned: dict[str, Any] = {}
        for key, value in payload.items():
            if _is_blocked_metadata_key(str(key)):
                continue
            cleaned[str(key)] = sanitize_public_payload(value)
        return cleaned

    if isinstance(payload, list):
        return [sanitize_public_payload(item) for item in payload]

    if isinstance(payload, tuple):
        return tuple(sanitize_public_payload(item) for item in payload)

    if isinstance(payload, str):
        return validate_output_token(payload)

    return payload


def sanitize_public_payload_with_allowlist(payload: Any, allowed_keys: Iterable[str]) -> Any:
    """
    Sanitize payload and enforce a strict key allowlist for top-level row objects.

    Intended for API routes that return list[dict] or dict payloads with known public fields.
    """
    allowed = {str(k) for k in allowed_keys}

    def _apply(value: Any) -> Any:
        cleaned = sanitize_public_payload(value)

        if isinstance(cleaned, dict):
            return {k: _apply(v) for k, v in cleaned.items() if k in allowed}

        if isinstance(cleaned, list):
            return [_apply(item) for item in cleaned]

        if isinstance(cleaned, tuple):
            return tuple(_apply(item) for item in cleaned)

        return cleaned

    return _apply(payload)


def client_identifier(request: Request) -> str:
    """
    Build a stable client identifier from forwarded IP (if present) or direct peer IP.
    """
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        first = forwarded.split(",")[0].strip()
        if first:
            return first
    return request.client.host if request.client else "unknown"


@dataclass
class SlidingWindowPolicy:
    limit: int
    window_seconds: int


class InMemorySlidingWindowRateLimiter:
    """
    Simple in-memory sliding-window rate limiter.
    Good for single-instance demo/MVP usage.
    """

    def __init__(self) -> None:
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def allow(self, key: str, policy: SlidingWindowPolicy) -> bool:
        now = time.time()
        cutoff = now - policy.window_seconds

        with self._lock:
            q = self._events[key]
            while q and q[0] < cutoff:
                q.popleft()
            if len(q) >= policy.limit:
                return False
            q.append(now)
            return True


RATE_LIMITER = InMemorySlidingWindowRateLimiter()


def enforce_rate_limit(
    *,
    request: Request,
    route_key: str,
    limit: int = 10,
    window_seconds: int = 60,
) -> None:
    """
    Enforce request rate limit for a client + route combination.

    Raises:
        SecurityValidationError: when rate limit is exceeded.
    """
    ident = client_identifier(request)
    composite_key = f"{route_key}:{ident}"
    allowed = RATE_LIMITER.allow(
        composite_key, SlidingWindowPolicy(limit=limit, window_seconds=window_seconds)
    )
    if not allowed:
        raise SecurityValidationError("Rate limit exceeded. Please try again shortly.")
