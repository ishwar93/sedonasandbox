"""stops.py — subway and bus stop endpoints"""

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from api.db import query
from api.security import (
    SecurityValidationError,
    enforce_rate_limit,
    sanitize_public_payload_with_allowlist,
)
from api.session import get_or_create_session_id, log_session_query

router = APIRouter(prefix='/api/stops', tags=['stops'])
STOP_ALLOWED_KEYS = {'stop_id', 'stop_name', 'lat', 'lon', 'feed_type'}


class Stop(BaseModel):
    stop_id: str
    stop_name: str
    lat: float
    lon: float
    feed_type: str


def _query_stops_from_combined(feed_type: str) -> list[dict]:
    return query(f"""
        SELECT DISTINCT
            location_id AS stop_id,
            location_name AS stop_name,
            lat,
            lon,
            location_type AS feed_type
        FROM transit.apitable_combined_locations
        WHERE location_type = '{feed_type}'
          AND lat IS NOT NULL
          AND lon IS NOT NULL
    """)


def _query_stops_from_gtfs(feed_type: str) -> list[dict]:
    location_filter = "1 OR gs.location_type IS NULL" if feed_type == "subway" else "0 OR gs.location_type IS NULL"
    return query(f"""
        SELECT DISTINCT
            gs.stop_id,
            gs.stop_name,
            gs.lat,
            gs.lon,
            '{feed_type}' AS feed_type
        FROM transit.gtfs_stops gs
        JOIN transit.gtfs_feed_versions gfv ON gs.feed_id = gfv.feed_id
        WHERE gfv.feed_type = '{feed_type}'
          AND (gs.location_type = {location_filter})
          AND gs.lat IS NOT NULL
          AND gs.lon IS NOT NULL
    """)


@router.get('/subway', response_model=list[Stop])
def get_subway_stops(request: Request):
    try:
        enforce_rate_limit(request=request, route_key='stops_subway')
        tables_used = ["transit.apitable_combined_locations"]
        try:
            rows = _query_stops_from_combined("subway")
        except Exception:
            rows = _query_stops_from_gtfs("subway")
            tables_used = ["transit.gtfs_stops", "transit.gtfs_feed_versions"]
        cleaned = sanitize_public_payload_with_allowlist(rows, STOP_ALLOWED_KEYS)
        sid = get_or_create_session_id(request)
        log_session_query(
            session_id=sid,
            question="Fetch subway stops",
            tables_used=tables_used,
            row_count=len(cleaned),
        )
        return cleaned
    except SecurityValidationError:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/bus', response_model=list[Stop])
def get_bus_stops(request: Request):
    try:
        enforce_rate_limit(request=request, route_key='stops_bus')
        tables_used = ["transit.apitable_combined_locations"]
        try:
            rows = _query_stops_from_combined("bus")
        except Exception:
            rows = _query_stops_from_gtfs("bus")
            tables_used = ["transit.gtfs_stops", "transit.gtfs_feed_versions"]
        cleaned = sanitize_public_payload_with_allowlist(rows, STOP_ALLOWED_KEYS)
        sid = get_or_create_session_id(request)
        log_session_query(
            session_id=sid,
            question="Fetch bus stops",
            tables_used=tables_used,
            row_count=len(cleaned),
        )
        return cleaned
    except SecurityValidationError:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
