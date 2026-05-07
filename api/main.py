"""
main.py — NYC Transit API
Run with: uv run uvicorn api.main:app --reload --port 8000
"""

import logging
import logging.config
import os

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.sessions import SessionMiddleware

from api.routers import alerts, ask, citibike, stops
from api.security import SecurityValidationError
from api.session import (
    SESSION_TTL_SECONDS,
    init_session_store,
    purge_expired_sessions,
)

from api.nl_sql.value_index import build_value_index

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# ---------------------------------------------------------------------------
# Logging
#
# LOG_LEVEL env var controls verbosity:
#   DEBUG   — every LLM call, SQL extraction, schema linker decision
#   INFO    — per-request pipeline steps (intent, linked tables, row counts)
#   WARNING — fallbacks, retries, non-fatal issues  <-- default
#   ERROR   — exceptions
# ---------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s %(levelname)-8s %(name)s — %(message)s",
            "datefmt": "%H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "stream": "ext://sys.stderr",
        },
    },
    "loggers": {
        "api": {
            "handlers": ["console"],
            "level": LOG_LEVEL,
            "propagate": False,
        },
        "databricks": {"level": "ERROR",   "propagate": True},
        "httpx":      {"level": "WARNING", "propagate": True},
        "httpcore":   {"level": "WARNING", "propagate": True},
        "urllib3":    {"level": "WARNING", "propagate": True},
    },
    "root": {
        "handlers": ["console"],
        "level": "WARNING",
    },
}

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)
logger.info("Logging configured — level=%s", LOG_LEVEL)

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="NYC Transit API", version="0.1.0")

session_secret = os.getenv("APP_SECRET_KEY", "dev-only-change-me")

app.add_middleware(
    SessionMiddleware,
    secret_key=session_secret,
    same_site="strict",
    https_only=False,
    max_age=SESSION_TTL_SECONDS,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:5174",
        "http://localhost:5175",
    ],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(stops.router)
app.include_router(citibike.router)
app.include_router(alerts.router)
app.include_router(ask.router)


@app.on_event("startup")
def on_startup() -> None:
    init_session_store()
    purge_expired_sessions()
    logger.info("Session store ready")
    build_value_index()   # LSH value index for WHERE-clause literal grounding


@app.exception_handler(SecurityValidationError)
def handle_security_validation_error(_, exc: SecurityValidationError):
    status_code = 429 if "rate limit" in str(exc).lower() else 400
    return JSONResponse(status_code=status_code, content={"detail": str(exc)})


@app.get("/health")
def health():
    return {"status": "ok"}
