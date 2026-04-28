import json
import os
import h3
from pathlib import Path

from databricks import sql
from dotenv import load_dotenv


load_dotenv()

DATABRICKS_HOST      = os.environ['DATABRICKS_HOST']
DATABRICKS_HTTP_PATH = os.environ['DATABRICKS_HTTP_PATH']
DATABRICKS_TOKEN     = os.environ['DATABRICKS_TOKEN']


def get_connection():
    return sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
)

def run_sql(label: str, sql_str: str, params: list | None = None):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                if params is not None:
                    cursor.execute(sql_str, params)
                else:
                    cursor.execute(sql_str)
        print(f"  ✓ {label}")
    except Exception as e:
        print(f"  ✗ {label}: {e}")

def fetch_rows(sql_str: str) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_str)
            if cursor.description is None:
                return []
            columns = [d[0] for d in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]


def row_to_geojson_feature(row: dict) -> dict:
    """One DB row -> GeoJSON Feature (Point). Coordinates are [lon, lat]."""
    lon = float(row["lon"])
    lat = float(row["lat"])
    props = {
        k: v
        for k, v in row.items()
        if k not in ("lat", "lon") and v is not None
    }
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": props,
    }


def write_geojsonseq(rows: list[dict], out_path: str | Path) -> int:
    """Write newline-delimited GeoJSON (one Feature per line) for tippecanoe."""
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            if row.get("lat") is None or row.get("lon") is None:
                continue
            f.write(json.dumps(row_to_geojson_feature(row), ensure_ascii=False) + "\n")
            n += 1
    return n


def get_transit_points_sql() -> list[dict]:
    return fetch_rows("""
    SELECT
        location_id,
        location_name,
        location_type,
        lat,
        lon,
        h3_r8,
        h3_r9
    FROM transit.apitable_combined_locations
    WHERE lat IS NOT NULL AND lon IS NOT NULL
    """)
    

def populate_transit_points_sql():
    """Rebuild combined locations: TRUNCATE + INSERT + OPTIMIZE (separate executes — multi-statement batches are unreliable)."""
    run_sql("truncate apitable_combined_locations", """
        TRUNCATE TABLE transit.apitable_combined_locations
    """)
    run_sql("insert apitable_combined_locations", """
        INSERT INTO transit.apitable_combined_locations
        SELECT
          station_id AS location_id,
          lat,
          lon,
          station_name AS location_name,
          'citibike' AS location_type,
          CAST(NULL AS BIGINT) AS h3_r8,
          CAST(NULL AS BIGINT) AS h3_r9
        FROM transit.citibike_stations
        GROUP BY station_id, station_name, lat, lon

        UNION ALL

        SELECT
          concat(s.feed_id, '_', s.stop_id) AS location_id,
          s.lat,
          s.lon,
          s.stop_name AS location_name,
          CASE
            WHEN v.feed_type = 'subway' THEN 'subway'
            WHEN v.feed_type = 'bus' THEN 'bus'
          END AS location_type,
          CAST(NULL AS BIGINT) AS h3_r8,
          CAST(NULL AS BIGINT) AS h3_r9
        FROM transit.gtfs_stops s
        INNER JOIN transit.gtfs_feed_versions v ON s.feed_id = v.feed_id
        WHERE v.feed_type IN ('subway', 'bus')
        GROUP BY
          s.feed_id,
          s.stop_id,
          s.stop_name,
          s.lat,
          s.lon,
          v.feed_type
    """)
    run_sql("optimize apitable_combined_locations", """
        OPTIMIZE transit.apitable_combined_locations
    """)


def export_transit_points_geojsonseq(out_path: str | Path = "tiles/transit_stops.geojsonseq") -> None:
    """Fetch combined locations and write GeoJSON Text Sequence for tippecanoe."""
    rows = get_transit_points_sql()
    n = write_geojsonseq(rows, out_path)
    print(f"  Wrote {n:,} features -> {out_path}")

def h3_cells(lat: float, lon: float) -> tuple[int, int]:
    """H3 cell IDs as BIGINT-compatible integers (matches Databricks h3_longlatash3)."""
    s8 = h3.latlng_to_cell(lat, lon, 8)
    s9 = h3.latlng_to_cell(lat, lon, 9)
    return (h3.str_to_int(s8), h3.str_to_int(s9))


def _merge_h3_batch(cursor, batch: list[tuple[str, int, int]]) -> None:
    """Apply one MERGE of (location_id, h3_r8, h3_r9) rows into apitable_combined_locations."""
    if not batch:
        return
    value_tuples = ", ".join("(?, ?, ?)" for _ in batch)
    flat: list = []
    for location_id, r8, r9 in batch:
        flat.extend([location_id, r8, r9])
    cursor.execute(
        f"""
        MERGE INTO transit.apitable_combined_locations AS t
        USING (
          SELECT * FROM VALUES {value_tuples}
          AS src(location_id, h3_r8, h3_r9)
        ) AS s
        ON t.location_id = s.location_id
        WHEN MATCHED THEN UPDATE SET
          t.h3_r8 = s.h3_r8,
          t.h3_r9 = s.h3_r9
        """,
        flat
    )


def sync_h3_columns_to_databricks(batch_size: int = 200) -> None:

    rows = fetch_rows("""
        SELECT location_id, lat, lon
        FROM transit.apitable_combined_locations
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """)
    updates: list[tuple[str, int, int]] = []
    for row in rows:
        try:
            r8, r9 = h3_cells(float(row["lat"]), float(row["lon"]))
            updates.append((str(row["location_id"]), r8, r9))
        except Exception:
            continue

    if not updates:
        print("  (no rows to update for H3)")
        return

    total = 0
    with get_connection() as conn:
        with conn.cursor() as cursor:
            for i in range(0, len(updates), batch_size):
                chunk = updates[i : i + batch_size]
                _merge_h3_batch(cursor, chunk)
                total += len(chunk)
                print(f"  H3 MERGE batch {total:,} / {len(updates):,}")
    print(f"  ✓ sync_h3_columns_to_databricks: {len(updates):,} rows")


if __name__ == "__main__":
    print("Populating transit points...")
    populate_transit_points_sql()
    print("Syncing H3 columns (Python h3-py → Databricks)...")
    sync_h3_columns_to_databricks()
    print("Exporting transit points to geojsonseq...")
    export_transit_points_geojsonseq()
