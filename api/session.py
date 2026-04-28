"""
session.py
SQLite-backed session context store with 24-hour TTL.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from threading import Lock
from typing import Any

from fastapi import Request


SESSIONS_DB_PATH = os.path.join(os.path.dirname(__file__), "sessions.db")
SESSION_ID_KEY = "session_id"
SESSION_TTL_SECONDS = 24 * 60 * 60
_DB_LOCK = Lock()
_PURGE_LOCK = Lock()
_LAST_PURGE_TS = 0


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(SESSIONS_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_session_store() -> None:
    with _DB_LOCK:
        conn = _connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                  session_id TEXT PRIMARY KEY,
                  created_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS session_queries (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  session_id TEXT NOT NULL,
                  ts INTEGER NOT NULL,
                  question TEXT NOT NULL,
                  sql_gen TEXT,
                  tables_used TEXT,
                  row_count INTEGER,
                  answer_id TEXT,
                  FOREIGN KEY(session_id) REFERENCES sessions(session_id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_session_queries_sid_ts "
                "ON session_queries(session_id, ts DESC)"
            )
            conn.commit()
        finally:
            conn.close()


def _cutoff_ts(now: int | None = None) -> int:
    current = int(time.time()) if now is None else int(now)
    return current - SESSION_TTL_SECONDS


def purge_expired_sessions() -> int:
    """
    Purge sessions and conversation rows older than 24h.
    Returns the number of deleted sessions.
    """
    cutoff = _cutoff_ts()
    with _DB_LOCK:
        conn = _connect()
        try:
            conn.execute("DELETE FROM session_queries WHERE ts < ?", (cutoff,))
            cur = conn.execute("DELETE FROM sessions WHERE created_at < ?", (cutoff,))
            conn.commit()
            return int(cur.rowcount or 0)
        finally:
            conn.close()


def purge_expired_sessions_if_due(min_interval_seconds: int = 300) -> int:
    """
    Purge at most once per interval to avoid per-request cleanup overhead.
    """
    global _LAST_PURGE_TS
    now = int(time.time())
    with _PURGE_LOCK:
        if now - _LAST_PURGE_TS < int(min_interval_seconds):
            return 0
        deleted = purge_expired_sessions()
        _LAST_PURGE_TS = now
        return deleted


def get_or_create_session_id(request: Request) -> str:
    """
    Ensure a stable server-side session id exists for this client.
    """
    purge_expired_sessions_if_due()
    existing = request.session.get(SESSION_ID_KEY)
    if existing:
        return str(existing)

    sid = uuid.uuid4().hex
    request.session[SESSION_ID_KEY] = sid

    with _DB_LOCK:
        conn = _connect()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO sessions(session_id, created_at) VALUES(?, ?)",
                (sid, int(time.time())),
            )
            conn.commit()
        finally:
            conn.close()
    return sid


def log_session_query(
    *,
    session_id: str,
    question: str,
    sql_gen: str | None = None,
    tables_used: list[str] | None = None,
    row_count: int | None = None,
    answer_id: str | None = None,
) -> None:
    """
    Store a query turn for conversational context and audit/debug.
    """
    now = int(time.time())
    tables_json = json.dumps(tables_used or [])
    with _DB_LOCK:
        conn = _connect()
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO sessions(session_id, created_at)
                VALUES(?, ?)
                """,
                (session_id, now),
            )
            conn.execute(
                """
                INSERT INTO session_queries(session_id, ts, question, sql_gen, tables_used, row_count, answer_id)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (session_id, now, question, sql_gen, tables_json, row_count, answer_id),
            )
            conn.commit()
        finally:
            conn.close()


def get_recent_session_queries(session_id: str, limit: int = 2) -> list[dict[str, Any]]:
    """
    Return the most recent query turns for prompt context (newest last).
    """
    with _DB_LOCK:
        conn = _connect()
        try:
            rows = conn.execute(
                """
                SELECT session_id, ts, question, sql_gen, tables_used, row_count, answer_id
                FROM session_queries
                WHERE session_id = ?
                ORDER BY ts DESC
                LIMIT ?
                """,
                (session_id, int(limit)),
            ).fetchall()
        finally:
            conn.close()

    result: list[dict[str, Any]] = []
    for row in reversed(rows):
        result.append(
            {
                "session_id": str(row["session_id"]),
                "ts": int(row["ts"]),
                "question": str(row["question"]),
                "sql_gen": row["sql_gen"],
                "tables_used": json.loads(row["tables_used"] or "[]"),
                "row_count": row["row_count"],
                "answer_id": row["answer_id"],
            }
        )
    return result
