
import os
import requests
import polars as pl
from databricks import sql
from datetime import datetime, timedelta
from dotenv import load_dotenv
from nyct_gtfs import NYCTFeed

load_dotenv()

DATABRICKS_HOST      = os.environ['DATABRICKS_HOST']
DATABRICKS_HTTP_PATH = os.environ['DATABRICKS_HTTP_PATH']
DATABRICKS_TOKEN     = os.environ['DATABRICKS_TOKEN']
MTA_BUS_KEY          = os.environ['MTA_BUS_KEY']

# One representative per feed group — no API key required since MTA opened access.
FEED_REPRESENTATIVES = ["1", "A", "B", "G", "J", "N", "L", "SIR"]


def get_connection():
    return sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )


def get_last_seen(source: str) -> str | None:
    """Return the last stored snapshot token for a source."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT etag FROM transit.gtfs_feed_versions
                    WHERE feed_id = 'poll_{source}'
                """)
                row = cursor.fetchone()
                return row[0] if row else None
    except Exception:
        return None


def set_last_seen(source: str, value: str):
    """Upsert the snapshot token for a source."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    DELETE FROM transit.gtfs_feed_versions
                    WHERE feed_id = 'poll_{source}'
                """)
                cursor.execute("""
                    INSERT INTO transit.gtfs_feed_versions
                    (feed_id, feed_type, source_url, etag, downloaded_at)
                    VALUES (?, ?, ?, ?, ?)
                """, [
                    f'poll_{source}', 'poll', source, value,
                    datetime.utcnow().isoformat()
                ])
    except Exception as e:
        print(f"  [set_last_seen:{source}] error: {e}")


# ── Generic append writer ──────────────────────────────────────────────────────
# Used by: citibike_status, bus_positions, traffic_speeds
# These are time-series tables — every poll adds new rows.
# Historical data feeds the agg_* aggregation tables.

def write_to_databricks(df: pl.DataFrame, table: str) -> int:
    if df.is_empty():
        print(f"  [{table}] no data to write")
        return 0

    rows       = df.to_dicts()
    cols       = list(rows[0].keys())
    col_names  = ', '.join(cols)
    batch_size = 2000
    total      = 0

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                for i in range(0, len(rows), batch_size):
                    batch        = rows[i:i + batch_size]
                    placeholders = ', '.join(f"({', '.join(['?' for _ in cols])})" for _ in batch)
                    values       = [v for r in batch for v in r.values()]
                    cursor.execute(
                        f"INSERT INTO transit.{table} ({col_names}) VALUES {placeholders}",
                        values
                    )
                    total += len(batch)
                    print(f"  {table}: {total:,}", end='\r')
        print()
        return total
    except Exception as e:
        print(f"\n  [{table}] write error: {e}")
        return 0


# ── Overwrite writer ───────────────────────────────────────────────────────────
# Used by: citibike_stations, subway_positions
# These are current-state tables — only the latest snapshot matters.
# Historic rows have no analytical value.

def overwrite_table(df: pl.DataFrame, table: str) -> int:
    if df.is_empty():
        print(f"  [{table}] no data to write")
        return 0

    rows       = df.to_dicts()
    cols       = list(rows[0].keys())
    col_names  = ', '.join(cols)
    batch_size = 500
    total      = 0

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DELETE FROM transit.{table} WHERE 1=1")
                for i in range(0, len(rows), batch_size):
                    batch        = rows[i:i + batch_size]
                    placeholders = ', '.join(f"({', '.join(['?' for _ in cols])})" for _ in batch)
                    values       = [v for r in batch for v in r.values()]
                    cursor.execute(
                        f"INSERT INTO transit.{table} ({col_names}) VALUES {placeholders}",
                        values
                    )
                    total += len(batch)
                    print(f"  {table}: {total:,}", end='\r')
        print()
        return total
    except Exception as e:
        print(f"\n  [{table}] write error: {e}")
        return 0


# ── Fetchers ───────────────────────────────────────────────────────────────────

def fetch_citibike() -> tuple[pl.DataFrame, pl.DataFrame]:
    now = datetime.utcnow()

    try:
        status_r = requests.get(
            "https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_status.json",
            timeout=5
        ).json()
        info_r = requests.get(
            "https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_information.json",
            timeout=5
        ).json()
    except Exception as e:
        print(f"  [citibike] fetch error: {e}")
        return pl.DataFrame(), pl.DataFrame()

    # Skip status write if feed hasn't updated (GBFS ttl=60s)
    status_ts = str(status_r.get('last_updated', ''))
    if status_ts and status_ts == get_last_seen('citibike_status'):
        print(f"  [citibike] unchanged (last_updated={status_ts}) — skipping")
        return pl.DataFrame(), pl.DataFrame()
    set_last_seen('citibike_status', status_ts)

    # status rows — time series, appended
    status_rows = [{
        'station_id':       str(s['station_id']),
        'bikes_available':  int(s.get('num_bikes_available', 0)),
        'ebikes_available': int(s.get('num_ebikes_available', 0)),
        'docks_available':  int(s.get('num_docks_available', 0)),
        'is_renting':       int(s.get('is_renting', 0)),
        'is_returning':     int(s.get('is_returning', 0)),
        'ingested_at':      now.isoformat()
    } for s in status_r['data']['stations']]

    # station rows — current state only, overwritten
    # No ingested_at — this is config, not a time series
    station_rows = [{
        'station_id':   str(s.get('station_id', '')),
        'station_name': str(s.get('name', '')),
        'lat':          float(s.get('lat', 0)),
        'lon':          float(s.get('lon', 0)),
        'capacity':     int(s.get('capacity', 0)),
    } for s in info_r['data']['stations']]

    return pl.DataFrame(status_rows), pl.DataFrame(station_rows)


def fetch_buses() -> pl.DataFrame:
    now = datetime.utcnow()

    try:
        resp = requests.get(
            "https://bustime.mta.info/api/siri/vehicle-monitoring.json",
            params={
                "key":     MTA_BUS_KEY,
                "version": 2,
                "VehicleMonitoringDetailLevel": "minimum"
            },
            timeout=10
        ).json()

        delivery = resp['Siri']['ServiceDelivery']['VehicleMonitoringDelivery']
        if isinstance(delivery, list):
            delivery = delivery[0]

        valid_until = delivery.get('ValidUntil', '')
        if valid_until and valid_until == get_last_seen('bus_positions'):
            print(f"  [bus] unchanged (ValidUntil={valid_until}) — skipping")
            return pl.DataFrame()
        set_last_seen('bus_positions', valid_until)

        activities = delivery.get('VehicleActivity', [])
        if isinstance(activities, dict):
            activities = [activities]

    except Exception as e:
        print(f"  [bus] fetch error: {e}")
        return pl.DataFrame()

    rows = []
    for activity in activities:
        try:
            j   = activity.get('MonitoredVehicleJourney', {})
            loc = j.get('VehicleLocation', {})
            if not loc:
                continue

            lat = float(loc.get('Latitude',  0))
            lon = float(loc.get('Longitude', 0))
            if lat == 0 or lon == 0:
                continue

            line_name = (
                j['PublishedLineName'][0]
                if isinstance(j.get('PublishedLineName'), list)
                else j.get('PublishedLineName', '')
            )

            mc   = j.get('MonitoredCall', {})
            caps = mc.get('Extensions', {}).get('Capacities', {})

            rows.append({
                'vehicle_ref':        str(j.get('VehicleRef', '')),
                'line_name':          str(line_name),
                'lat':                lat,
                'lon':                lon,
                'expected_arrival':   str(mc.get('ExpectedArrivalTime', '')),
                'distance_from_stop': int(mc.get('DistanceFromStop', 0) or 0),
                'stops_away':         int(mc.get('NumberOfStopsAway', 0) or 0),
                'passenger_count':    int(caps.get('EstimatedPassengerCount', 0) or 0),
                'passenger_capacity': int(caps.get('EstimatedPassengerCapacity', 0) or 0),
                'ingested_at':        now.isoformat()
            })

        except Exception as e:
            print(f"  [bus] activity error: {e}")
            continue

    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fetch_subway() -> pl.DataFrame:
    now  = datetime.utcnow()
    rows = []

    for line in FEED_REPRESENTATIVES:
        try:
            feed = NYCTFeed(line)

            feed_key       = f'subway_{line}'
            last_generated = feed.last_generated.isoformat() if feed.last_generated else ''
            if last_generated and last_generated == get_last_seen(feed_key):
                print(f"  [subway:{line}] unchanged — skipping")
                continue
            set_last_seen(feed_key, last_generated)

            for train in feed.trips:
                # All trains included — even without a location fix yet.
                # location_stop='' for unfixed trains; PageRank still counts
                # them as active trips on their route for frequency weighting.
                next_arrival = ''
                if train.stop_time_updates:
                    next_stop    = train.stop_time_updates[0]
                    next_arrival = (
                        next_stop.arrival.isoformat()
                        if next_stop.arrival else ''
                    )

                rows.append({
                    'trip_id':         str(train.trip_id or ''),
                    'route_id':        str(train.route_id or ''),
                    'direction':       str(train.direction or ''),
                    'location_stop':   str(train.location or ''),
                    'location_status': str(train.location_status or ''),
                    'next_arrival':    next_arrival,
                    'last_update':     (
                        train.last_position_update.isoformat()
                        if train.last_position_update else ''
                    ),
                    'ingested_at':     now.isoformat()
                })

        except Exception as e:
            print(f"  [subway] line {line} error: {e}")
            continue

    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fetch_traffic() -> pl.DataFrame:
    now = datetime.utcnow()

    try:
        data = requests.get(
            "https://data.cityofnewyork.us/resource/i4gi-tjb9.json"
            "?$limit=1000&$order=data_as_of+DESC",
            timeout=5
        ).json()
    except Exception as e:
        print(f"  [traffic] fetch error: {e}")
        return pl.DataFrame()

    latest_ts = data[0].get('data_as_of', '') if data else ''
    if latest_ts and latest_ts == get_last_seen('traffic_speeds'):
        print(f"  [traffic] unchanged (data_as_of={latest_ts}) — skipping")
        return pl.DataFrame()
    set_last_seen('traffic_speeds', latest_ts)

    rows = []
    for seg in data:
        try:
            rows.append({
                'segment_id':  str(seg.get('id', '')),
                'speed':       float(seg.get('speed', 0) or 0),
                'travel_time': int(float(seg.get('travel_time', 0) or 0)),
                'borough':     str(seg.get('borough', '')),
                'link_points': str(seg.get('link_points', '')),
                'data_as_of':  str(seg.get('data_as_of', '')),
                'ingested_at': now.isoformat()
            })
        except Exception as e:
            print(f"  [traffic] segment error: {e}")
            continue

    return pl.DataFrame(rows) if rows else pl.DataFrame()


# ── Rolling deletion ───────────────────────────────────────────────────────────
# Only time-series tables need rolling delete.
# subway_positions is overwritten each poll so never accumulates.
# citibike_stations is overwritten each poll so never accumulates.

def rolling_delete(days_to_keep: int = 30):
    if datetime.utcnow().hour != 0:
        return

    cutoff = (datetime.utcnow() - timedelta(days=days_to_keep)).isoformat()
    tables = ['citibike_status', 'bus_positions', 'traffic_speeds']

    print(f"\n  Rolling delete — removing rows before {cutoff}")

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                for table in tables:
                    cursor.execute(f"""
                        DELETE FROM transit.{table}
                        WHERE ingested_at < '{cutoff}'
                    """)
                    print(f"  Deleted old rows from transit.{table}")
    except Exception as e:
        print(f"  Rolling delete error: {e}")


def check_row_counts():
    tables = [
        'citibike_stations', 'citibike_status',
        'bus_positions', 'subway_positions', 'traffic_speeds'
    ]
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM transit.{table}")
                    count = cursor.fetchone()[0]
                    print(f"  transit.{table:25s} {count:>10,} rows")
    except Exception as e:
        print(f"  Row count error: {e}")


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    start = datetime.utcnow()
    print(f"\n{'='*50}")
    print(f"Poll run: {start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"{'='*50}")

    print("\nFetching Citi Bike...")
    status_df, station_df = fetch_citibike()
    n = write_to_databricks(status_df, 'citibike_status')       # append — time series
    print(f"  citibike_status:   {n:>5} rows written")
    n = overwrite_table(station_df, 'citibike_stations')        # overwrite — current state
    print(f"  citibike_stations: {n:>5} rows written")

    print("\nFetching MTA Bus...")
    n = write_to_databricks(fetch_buses(), 'bus_positions')     # append — time series
    print(f"  bus_positions:     {n:>5} rows written")

    print("\nFetching MTA Subway...")
    n = overwrite_table(fetch_subway(), 'subway_positions')     # overwrite — current state
    print(f"  subway_positions:  {n:>5} rows written")

    print("\nFetching NYC DOT Traffic...")
    n = write_to_databricks(fetch_traffic(), 'traffic_speeds')  # append — time series
    print(f"  traffic_speeds:    {n:>5} rows written")

    rolling_delete(days_to_keep=30)

    print("\nCurrent row counts:")
    check_row_counts()

    elapsed = (datetime.utcnow() - start).seconds
    print(f"\nCompleted in {elapsed}s")
    print(f"{'='*50}\n")