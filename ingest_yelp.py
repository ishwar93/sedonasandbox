import os
import json
import polars as pl
from databricks import sql
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATABRICKS_HOST      = os.environ['DATABRICKS_HOST']
DATABRICKS_HTTP_PATH = os.environ['DATABRICKS_HTTP_PATH']
DATABRICKS_TOKEN     = os.environ['DATABRICKS_TOKEN']

YELP_FILE = Path(__file__).parent / "data" / "yelp" / \
            "yelp_academic_dataset_business.json"

NYC_LAT_MIN, NYC_LAT_MAX = 40.4, 40.9
NYC_LON_MIN, NYC_LON_MAX = -74.2, -73.7

def get_connection():
    return sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path = DATABRICKS_HTTP_PATH,
        access_token = DATABRICKS_TOKEN
    )

def write_to_databricks(df: pl.DataFrame, table: str):
    if df.is_empty():
        return 0

    rows = df.to_dicts()
    cols = list(rows[0].keys())
    col_names = ', '.join(cols)
    placeholders = ','.join(['?' for _ in cols])
    batch_size = 500
    total = 0

    try:

        with get_connection() as conn:
            with conn.cursor() as cursor:
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i+batch_size]
                    cursor.executemany(
                        f"INSERT INTO transit.{table}"
                        f"({col_names}) VALUES ({placeholders})",
                        [list(r.values()) for r in batch]
                    )
                    total += len(batch)
                    print(f"{table}: {total:,} rows written", end='\r')
    
    except Exception as e:
        print(f"\n [{table}] write error: {e}")

        return 0

    return total

def ingest_yelp_businesses():
    if not YELP_FILE.exists():
        print(f"Error: Yelp file not found at {YELP_FILE}")
        return 0

    now = datetime.utcnow()
    rows = []
    skipped = 0

    print(f"Reading {YELP_FILE.name}...")

    with open(YELP_FILE, 'r') as f:
        for line in f:
            try:
                b = json.loads(line.strip())
                if not b:
                    continue

                lat = b.get('latitude')
                lon = b.get('longitude')

                if lat is None or lon is None:
                    skipped += 1
                    continue

                lat = float(lat)
                lon = float(lon)

                if not (NYC_LAT_MIN <= lat <= NYC_LAT_MAX and 
                        NYC_LON_MIN <= lon <= NYC_LON_MAX):
                    skipped += 1
                    continue

                categories = b.get('categories') or ''
                if isinstance(categories, list):
                    categories = ', '.join(categories)

                    rows.append({
                        'business_id': str(b.get('business_id', '')),
                        'business_name': str(b.get('name', '')),
                        'lat': lat,
                        'lon': lon,
                        'stars': float(b.get('stars', 0)or 0),
                        'is_open': int(b.get('is_open', 0) or 0),
                        'categories': str(categories)
                    })

            except Exception as e:
                print(f"Error processing line: {e}")
                continue

    print(f"Parsed {len(rows):,} NYC businesses"
          f"({skipped:,} outside NYC or missing coords)")

    if not rows:
        return 0 

    df = pl.DataFrame(rows)
    return write_to_databricks(df, 'yelp_businesses')

if __name__ == "__main__":
    n = ingest_yelp_businesses()
    print("Done - {n:,} businesses written")

        



    

