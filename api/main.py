"""
main.py — NYC Transit API
Run with: uv run uvicorn api.main:app --reload --port 8000
"""

import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from dotenv import load_dotenv

from api.routers import stops, citibike, alerts, ask
from api.security import SecurityValidationError
from api.session import (
    SESSION_TTL_SECONDS,
    init_session_store,
    purge_expired_sessions,
)

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

app = FastAPI(title='NYC Transit API', version='0.1.0')

session_secret = os.getenv('APP_SECRET_KEY')
if not session_secret:
    session_secret = "dev-only-change-me"

app.add_middleware(
    SessionMiddleware,
    secret_key=session_secret,
    same_site='strict',
    https_only=False,
    max_age=SESSION_TTL_SECONDS,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        'http://localhost:5173',
        'http://localhost:5174',
        'http://localhost:5175',
    ],
    allow_methods=['GET'],
    allow_headers=['*'],
)

app.include_router(stops.router)
app.include_router(citibike.router)
app.include_router(alerts.router)
app.include_router(ask.router)


@app.on_event("startup")
def on_startup() -> None:
    init_session_store()
    purge_expired_sessions()


@app.exception_handler(SecurityValidationError)
def handle_security_validation_error(_, exc: SecurityValidationError):
    status_code = 429 if "rate limit" in str(exc).lower() else 400
    return JSONResponse(status_code=status_code, content={'detail': str(exc)})


@app.get('/health')
def health():
    return {'status': 'ok'}
