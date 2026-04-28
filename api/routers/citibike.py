"""citibike.py — Citibike station endpoints"""

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from api.db import query
from api.security import (
    SecurityValidationError,
    enforce_rate_limit,
    sanitize_public_payload_with_allowlist,
)
from api.session import get_or_create_session_id, log_session_query

router = APIRouter(prefix='/api/citibike', tags=['citibike'])
CITIBIKE_ALLOWED_KEYS = {'station_id', 'station_name', 'lat', 'lon', 'capacity'}


class CitibikeStation(BaseModel):
    station_id: str
    station_name: str
    lat: float
    lon: float
    capacity: int


def _query_citibike_from_combined() -> list[dict]:
    return query("""
        SELECT DISTINCT
            c.location_id AS station_id,
            c.location_name AS station_name,
            c.lat,
            c.lon,
            s.capacity
        FROM transit.apitable_combined_locations c
        LEFT JOIN transit.citibike_stations s ON c.location_id = s.station_id
        WHERE c.location_type = 'citibike'
          AND c.lat IS NOT NULL
          AND c.lon IS NOT NULL
    """)


def _query_citibike_direct() -> list[dict]:
    return query("""
        SELECT
            station_id,
            station_name,
            lat,
            lon,
            capacity
        FROM transit.citibike_stations
        WHERE lat IS NOT NULL
          AND lon IS NOT NULL
    """)


@router.get('/stations', response_model=list[CitibikeStation])
def get_citibike_stations(request: Request):
    try:
        enforce_rate_limit(request=request, route_key='citibike_stations')
        tables_used = ["transit.apitable_combined_locations", "transit.citibike_stations"]
        try:
            rows = _query_citibike_from_combined()
        except Exception:
            rows = _query_citibike_direct()
            tables_used = ["transit.citibike_stations"]
        cleaned = sanitize_public_payload_with_allowlist(rows, CITIBIKE_ALLOWED_KEYS)
        sid = get_or_create_session_id(request)
        log_session_query(
            session_id=sid,
            question="Fetch Citi Bike stations",
            tables_used=tables_used,
            row_count=len(cleaned),
        )
        return cleaned
    except SecurityValidationError:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
