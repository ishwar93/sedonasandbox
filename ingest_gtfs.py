import os
import io
import zipfile
import requests
import polars as pl
from databricks import sql
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

DATABRICKS_HOST      = os.environ['DATABRICKS_HOST']
DATABRICKS_HTTP_PATH = os.environ['DATABRICKS_HTTP_PATH']
DATABRICKS_TOKEN     = os.environ['DATABRICKS_TOKEN']

FEEDS = {
    'subway_supplemented': 'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip',
    'subway_regular':      'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip',
    'bus_bronx':           'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_bx.zip',
    'bus_brooklyn':        'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_b.zip',
    'bus_manhattan':       'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_m.zip',
    'bus_queens':          'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_q.zip',
    'bus_staten_island':   'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_si.zip',
    'bus_mta_company':     'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_busco.zip',
}

def get_connection():
    return sql.connect(
        server_hostname = DATABRICKS_HOST,
        http_path       = DATABRICKS_HTTP_PATH,
        access_token    = DATABRICKS_TOKEN
    )

 def write_to_databricks(df: pl.DataFrame, table: str) -> int:
    if df.is_empty():
        return 0

    rows         = df.to_dicts()
    cols         = list(rows[0].keys())
    col_names    = ', '.join(cols)
    placeholders = ', '.join(['?' for _ in cols])
    batch_size   = 500
    total        = 0

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i + batch_size]
                    cursor.executemany(
                        f"INSERT INTO transit.{table} "
                        f"({col_names}) VALUES ({placeholders})",
                        [list(r.values()) for r in batch]
                    )
                    total += len(batch)
                    print(f"  {table}: {total:,}", end='\r')
    except Exception as e:
        print(f"\n  [{table}] write error: {e}")
        return 0

    print()
    return total

def delete_feed(feed_id: str):
    """Delete all rows for this feed before reinserting."""
    tables = [
        'gtfs_stops', 'gtfs_routes', 'gtfs_trips',
        'gtfs_transfers', 'gtfs_stop_connections',
        'gtfs_calendar', 'gtfs_shapes'
    ]
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                for table in tables:
                    cursor.execute(f"""
                        DELETE FROM transit.{table}
                        WHERE feed_id = '{feed_id}'
                    """)
    except Exception as e:
        print(f"  Delete error for {feed_id}: {e}")   

def get_remote_etag(url: str) -> str | None:
    try:
        resp = requests.get(url, timeout=10)
        return resp.headers.get('ETag', '').strip('"')
    except Exception as e:
        print(f" Etag check failed: {e}")
        return None

def get_stored_etag(feed_id: str) -> str | None:
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT etag FROM transit.gtfs_feed_versions
                    WHERE feed_id = '{feed_id}'
                """)
                row = cursor.fetchone()
                return row[0] if row else None
    except Exception as e:
        return None

def update_feed_version(feed_id: str, url: str, etag: str):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    DELETE FROM transit.gtfs_feed_versions
                    WHERE feed_id = '{feed_id}'
                """)
                cursor.execute(f"""
                    INSERT INTO transit.gtfs_feed_versions (feed_id, url, etag)
                    (feed_id, feed_type, source_url, etag, downloaded_at)
                    VALUES(?,?,?,?,?)
                """, [
                    feed_id,
                    'subway' if 'subway' in feed_id else 'bus',
                    url,
                    etag,
                    datetime.utcnow().isoformat()
                ])

    except Exception as e:
        print(f"  Version update failed: {e}")

def ingest_stops(zf: zipfile.Zipfile, feed_id: str):
    if 'stops.txt' not in zf.namelist():
        print(f"  [ERROR] No stops.txt in {feed_id}")
        return 0

    with zf.open('stops.txt', 'rb') as f:
        content = f.read()
        try:
            raw = content.decode('utf-8')
        except Exception :
            raw = content.decode('latin-1')

        df = pl.read_csv(io.StringIO(raw))

    now = datetime.utcnow().isoformat()

    rename_map = {
        'stop_id': 'stop_id',
        'stop_name': 'stop_name',
        'stop_lat': 'lat',
        'stop_lon': 'lon',
        'location_type': 'location_type',
        'parent_station': 'parent_station'
    }

    available = {k: v for k, v in rename_map.items() if k in df.columns}
    df  = df.select(list(available.keys())).rename(available) #filter and rename columns
    df = df.with_columns([
        pl.lit(feed_id).alias('feed_id'),
        pl.lit(now).alias('ingested_at'),
        pl.col('lat').cast (pl.Float64),
        pl.col('lon').cast(pl.Float64),
        pl.col('location_type').cast(pl.Int32)
        pl.col('stop_id').cast(pl.Utf8)
        pl.col('stop_name').cast(pl.Utf8)
        pl.col('parent_station').cast(pl.Utf8)
    ])
    return write_to_databricks(df, 'gtfs_stops')

def ingest_routes(zf: zipfile.Zipfile, feed_id: str):
    if 'routes.txt' not in zf.namelist():
        print(f"  [ERROR] No stops.txt in {feed_id}")
        return 0

    with zf.open('routes.txt', 'rb') as f:
        content = f.read()
        try:
            raw = content.decode('utf-8')
        except Exception :
            raw = content.decode('latin-1')

        df = pl.read_csv(io.StringIO(raw))

    now = datetime.utcnow().isoformat()

    rename_map = {
        'route_id': 'route_id',
        'agency_id': 'agency_id',
        'route_short_name': 'route_short_name',
        'route_type': 'route_type'
    }

    available = {k: v for k, v in rename_map.items() if k in df.columns}
    df  = df.select(list(available.keys())).rename(available) #filter and rename columns
    if 'agency_id' not in df.columns:
        df = df.with_columns(pl.lit('').alias('agency_id'))
    df = df.with_columns([
        pl.lit(feed_id).alias('feed_id'),
        pl.lit(now).alias('ingested_at'),
        pl.col('lat').cast (pl.Float64),
        pl.col('route_short_name').cast(pl.Int32)
    ])
    return write_to_databricks(df, 'gtfs_routes')

def ingest_trips(zf: zipfile.Zipfile, feed_ids:str):
    if 'trips.txt' not in zf.namelist():
        return 0
    with zf.open('trips.txt', 'rb') as f:
        content = f.read()
        try:
            raw = content.decode('utf-8')
        except Exception :
            raw = content.decode('latin-1')

        df = pl.read_csv(io.StringIO(raw))

    now = datetime.utcnow().isoformat()

    rename_map = {
        'trip_id': 'trip_id',
        'route_id': 'route_id',
        'service_id': 'service_id',
        'direction_id': 'direction_id'
    }

    available = {k: v for k, v in rename_map.items() if k in df.columns}
    df  = df.select(list(available.keys())).rename(available) #filter and rename columns
    if 'direction_id' not in df.columns:
        df = df.with_columns(pl.lit(0).alias('direction_id'))
    df = df.with_columns([
        pl.lit(feed_id).alias('feed_id'),
        pl.lit(now).alias('ingested_at'),
        pl.col('direction_id').cast(pl.Int32)
    ])
    return write_to_databricks(df, 'gtfs_trips')
    
def ingest_transfers(zf: zipfile.Zipfile, feed_id: str):
        if 'transfers.txt' not in zf.namelist():
        return 0
    with zf.open('transfers.txt', 'rb') as f:
        content = f.read()
        try:
            raw = content.decode('utf-8')
        except Exception :
            raw = content.decode('latin-1')

        df = pl.read_csv(io.StringIO(raw))

    now = datetime.utcnow().isoformat()

    rename_map = {
        'from stop_id': 'from_stop_id',
        'to_stop_id': 'to_stop_id',
        'transfer_type': 'transfer_type',
        'min_trasfer_time': 'min_transfer_time'
    }

    available = {k: v for k, v in rename_map.items() if k in df.columns}
    df  = df.select(list(available.keys())).rename(available) #filter and rename columns
    if 'min_transfer_time' not in df.columns:
        df = df.with_columns(pl.lit(None).alias('min_transfer_time'))
    df = df.with_columns([
        pl.lit(now).alias('ingested_at'),
        pl.col('transfer_type').cast(pl.Int32)
    ])
    return write_to_databricks(df, 'gtfs_transfers')

def ingest_calendar(zf: zipfile.Zipfile, feed_id: str):
    if 'calendar.txt' not in zf.namelist():
        return 0
    with zf.open('calendar.txt', 'rb') as f:
        content = f.read()
        try:
            raw = content.decode('utf-8')
        except Exception :
            raw = content.decode('latin-1')

        df = pl.read_csv(io.StringIO(raw))

    now = datetime.utcnow().isoformat()

    cols_needed = [
        'service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'
    ]

    available = [col for col in cols_needed if col in df.columns]
    df  = df.select(available) #filter columns

    for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
        if day in df.columns:
            df = df.with_columns(pl.col(day).cast(pl.Int32))
    df = df.with_columns([
        pl.lit(now).alias('ingested_at'),
    ])
    return write_to_databricks(df, 'gtfs_calendar')

def ingest_shapes(zf: zipfile.Zipfile, feed_id: str):
    if 'shapes.txt' not in zf.namelist():
        return 0

    with zf.open('shapes.txt') as f:
        df = pl.read_csv(f)

    now = datetime.utcnow().isoformat()

    rename_map = [
        'shape_id': 'shape_id',
        'shape_pt_lat': 'shape_pt_lat',
        'shape_pt_lon': 'shape_pt_lon',
        'shape_pt_sequence': 'shape_pt_sequence'
    ]
    available = {k: v for k,v in rename_map.items() if k in df.columns}
    df = df.select(list(available.keys())).rename(available)


    df = df.with_columns([
        pl.lit(now).alias('ingested_at'),
        pl.col('shape_pt_lat').cast(pl.Float64),
        pl.col('shape_pt_lon').cast(pl.Float64),
        pl.col('shape_pt_sequence').cast(pl.Int32)
    ])

    return write_to_databricks(df, 'gtfs_shapes')
    
def derive_stop_connections(zf: zipfile.Zipfile, feed_id: str):
    if 'stop_times.txt' not in zf.namelist():
        return 0
    with zf.open('stop_times.txt', 'rb') as f:
        content = f.read()
        try:
            raw = content.decode('utf-8')
        except Exception :
            raw = content.decode('latin-1')

        df_stoptimes = pl.read_csv(io.StringIO(raw))
        df_stoptimes = df_stoptimes.select(['trip_id', 'stop_id', 'stop_sequence', 'arrival_time', 'departure_time'])

    with zf.open('trips.txt', 'rb') as f:
        content = f.read()
        try:
            raw = content.decode('utf-8')
        except Exception :
            raw = content.decode('latin-1')
        
    df_trips = pl.read_csv(io.StringIO(raw))
    df_trips = df_trips.select(['trip_id', 'route_id', 'direction_id'])

    if 'direction_id' not in df_trips.columns:
        df_trips = df_trips.with_columns(pl.lit(0).alias('direction_id'))

    # join stop_times to trips to get route_id per row
    st = df_stoptimes.join(df_trips, on='trip_id', how='left')

    st = st.sort(['trip_id', 'stop_sequence'])

    st = st.with_columns([
        pl.col('stop_id').shift(-1).over('trip_id').alias('to_stop_id'),
        pl.col('arrival_time').shift(-1).over('trip_id').alias('next_arrival_time')
    ])

    st = st.filter(pl.col('to_stop_id').is_not_null())

    def time_to_sec(t: str):
        try:
            h, m, s = str(t).split(':')

            return int(h) * 3600 + int(m)*60 + int(s)

        except Exception :
            return 0 

    st = st.with_columns([
        pl.col('departure_time').map_elements(time_to_sec, return_dtype=pl.Int32).alias('dep_sec'), 
        pl.col('next_arrival_time').map_elements(time_to_sec, return_dtype=pl.Int32).alias('arr_sec')
    ]).with_columns([
        (pl.col('arr_sec') - pl.col('dep_sec')).alias('scheduled_travel_time_sec')
    ])

    now = datetime.utcnow().isoformat()

    connections = st.filter(
        pl.col('scheduled_travel_time_sec')>0
    ).select([
        pl.col('stop_id').alias('from_stop_id'),
        pl.col('to_stop_id'),
        pl.col('route_id'),
        pl.col('direction_id').cast(pl.Int32),
        pl.col('scheduled_travel_time_sec'),
    ]).unique(
        subset=['from_stop_id', 'to_stop_id', 'route_id', 'direction_id']
    ).with_columns([
        pl.lit(now).alias('ingested_at')
    ])

    print(f"  [INFO] {feed_id}: {len(connections)} stop connections derived")
    return write_to_databricks(connections, 'gtfs_stop_connections')

def ingest_feed(feed_id: str, url: str):
    print(f"\n{'-'*50}")
    print(f"Checking {feed_id}")

    remote_etag = get_remote_etag(url)
    stored_etag = get_stored_etag(feed_id)

    if remote_etag and remote_etag == stored_etag:
        print(f"Unchanged(etag={remote_etag[:16]}...) - skipping")
        return

    print(f"changed - downloading {url}")
    delete_feed(feed_id)

    n = ingest_stops(zf, feed_id)
    print(f"stops: {n:,} rows")

    n = ingest_routes(zf, feed_id)
    print(f"routes: {n:,} rows")

    n = ingest_trips(zf, feed_id)
    print(f"trips: {n:,} rows")

    n = ingest_transfers(zf, feed_id)
    print(f"transfers: {n:,} rows")

    n = ingest_calendar(zf, feed_id)
    print(f"calendar: {n:,} rows")

    n = ingest_shapes(zf, feed_id)
    print(f"shapes: {n:,} rows")

    n = derive_stop_connections(zf, feed_id)
    print(f"stop connections: {n:,} rows")

    if remote_etag:
        update_feed_version(feed_id, url, remote_etag)
        print(f"Version record updated")


if __name__ == '__main__':
    start = datetime.utcnow()

    print(f"\n{'-'*50}")
    print(f"Ingesting GTFS feeds")
    print(f"Started at {start.isoformat()}")
    print(f"'='*50")

    for feed_id, url in FEEDS.items():
        ingest_feed(feed_id, url)

        elapsed = (datetime.utcnow() - start).seconds

        print(f"  {feed_id}: {elapsed:,} seconds")
        print(f"  {'='*50}\n")



        