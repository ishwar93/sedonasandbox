"""alerts.py — Service alert endpoints"""

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from api.db import query
from api.security import (
    SecurityValidationError,
    enforce_rate_limit,
    sanitize_public_payload_with_allowlist,
)
from api.session import get_or_create_session_id, log_session_query

router = APIRouter(prefix='/api/alerts', tags=['alerts'])
ALERT_ALLOWED_KEYS = {
    'alert_id',
    'feed_source',
    'header_text_plain',
    'mercury_alert_type',
    'is_planned',
    'is_active_now',
    'first_seen_at',
    'last_seen_at',
}
ALERT_ENTITY_ALLOWED_KEYS = {'alert_id', 'agency_id', 'route_id', 'stop_id', 'priority_level'}


class Alert(BaseModel):
    alert_id: str
    feed_source: str
    header_text_plain: str | None
    mercury_alert_type: str | None
    is_planned: bool | None
    is_active_now: bool | None
    first_seen_at: str | None
    last_seen_at: str | None


class AlertEntity(BaseModel):
    alert_id: str
    agency_id: str | None
    route_id: str | None
    stop_id: str | None
    priority_level: int | None


@router.get('/active', response_model=list[Alert])
def get_active_alerts(request: Request):
    try:
        enforce_rate_limit(request=request, route_key='alerts_active')
        rows = query("""
            SELECT
                alert_id,
                feed_source,
                header_text_plain,
                mercury_alert_type,
                is_planned,
                is_active_now,
                CAST(first_seen_at AS STRING) AS first_seen_at,
                CAST(last_seen_at  AS STRING) AS last_seen_at
            FROM transit.service_alerts
            WHERE is_active_now = true
            ORDER BY last_seen_at DESC
        """)
        cleaned = sanitize_public_payload_with_allowlist(rows, ALERT_ALLOWED_KEYS)
        sid = get_or_create_session_id(request)
        log_session_query(
            session_id=sid,
            question="Fetch active alerts",
            tables_used=["transit.service_alerts"],
            row_count=len(cleaned),
        )
        return cleaned
    except SecurityValidationError:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/entities', response_model=list[AlertEntity])
def get_alert_entities(request: Request):
    """Affected routes/stops per active alert — used by PageRank edge weighting."""
    try:
        enforce_rate_limit(request=request, route_key='alerts_entities')
        rows = query("""
            SELECT
                sae.alert_id,
                sae.agency_id,
                sae.route_id,
                sae.stop_id,
                sae.priority_level
            FROM transit.service_alert_affected_entities sae
            JOIN transit.service_alerts sa ON sae.alert_id = sa.alert_id
            WHERE sa.is_active_now = true
        """)
        cleaned = sanitize_public_payload_with_allowlist(rows, ALERT_ENTITY_ALLOWED_KEYS)
        sid = get_or_create_session_id(request)
        log_session_query(
            session_id=sid,
            question="Fetch alert entities",
            tables_used=["transit.service_alert_affected_entities", "transit.service_alerts"],
            row_count=len(cleaned),
        )
        return cleaned
    except SecurityValidationError:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
