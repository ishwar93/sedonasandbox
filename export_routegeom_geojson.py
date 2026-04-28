import json
import os
from datetime import datetime
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
OUT_DIR = "build"
OUT_FILE = os.path.join(OUT_DIR, "transit_routegeom.geojsonseq")

QUERY = """
SELECT
  feed_id,
  shape_id,
  route_id,
  route_short_name,
  route_color,
  point_count,
  st_asgeojson(line_geometry) AS geometry_geojson
FROM transit.apitable_routegeom
WHERE line_geometry IS NOT NULL
"""

def log(message: str) -> None:
  ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
  print(f"[{ts} UTC] {message}", flush=True)

def main() -> None:
  start = datetime.utcnow()
  log("Starting route geometry export")
  os.makedirs(OUT_DIR, exist_ok=True)
  log(f"Ensured output directory exists: {OUT_DIR}")
  log(f"Output file: {OUT_FILE}")

  row_count = 0
  skipped_null_geom = 0
  total_fetched = 0
  batch_num = 0
  batch_size = 2000

  log("Connecting to Databricks")
  with sql.connect(
    server_hostname=DATABRICKS_HOST,
    http_path=DATABRICKS_HTTP_PATH,
    access_token=DATABRICKS_TOKEN,
  ) as conn:
    log("Connected to Databricks")
    with conn.cursor() as cursor:
      log("Executing query")
      cursor.execute(QUERY)
      log("Query submitted, streaming rows in batches")
      with open(OUT_FILE, "w", encoding="utf-8") as f:
        log("Opened output file for writing")
        while True:
          rows = cursor.fetchmany(batch_size)
          if not rows:
            break
          batch_num += 1
          total_fetched += len(rows)
          batch_written = 0
          batch_skipped = 0
          log(f"Fetched batch {batch_num}: {len(rows)} rows (total fetched: {total_fetched})")
          for (
            feed_id,
            shape_id,
            route_id,
            route_short_name,
            route_color,
            point_count,
            geometry_geojson,
          ) in rows:
            if geometry_geojson is None:
              skipped_null_geom += 1
              batch_skipped += 1
              continue
            feature = {
              "type": "Feature",
              "geometry": json.loads(geometry_geojson),
              "properties": {
                "feed_id": feed_id,
                "shape_id": shape_id,
                "route_id": route_id,
                "route_short_name": route_short_name,
                "route_color": route_color,
                "point_count": point_count,
              },
            }
            f.write(json.dumps(feature, separators=(",", ":")) + "\n")
            row_count += 1
            batch_written += 1
          log(
            f"Processed batch {batch_num}: wrote {batch_written}, skipped {batch_skipped} "
            f"(running total written: {row_count})"
          )

  elapsed = (datetime.utcnow() - start).total_seconds()
  log(
    f"Completed export in {elapsed:.1f}s: wrote {row_count} features to {OUT_FILE}; "
    f"skipped {skipped_null_geom} rows with null geometry"
  )
if __name__ == "__main__":
  main()