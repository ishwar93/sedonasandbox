import os
import time
import requests
import polars as pl
from databricks import sql
from datetime import datetime, timedelta
from google.transit import gtfs_realtime_pb2
from dotenv import load_dotenv
from nyct_gtfs import NYCTFeed
# //load environment variables from .env file into os.environ
load_dotenv() 

DATABRICKS_HOST = os.environ['DATABRICKS_HOST']
DATABRICKS_HTTP_PATH = os.environ['DATABRICKS_HTTP_PATH']
DATABRICKS_TOKEN = os.environ['DATABRICKS_TOKEN']
MTA_BUS_KEY = os.environ['MTA_BUS_KEY']
FEED_REPRESENTATIVES = ["1", "A", "B", "G", "J", "N", "L", "SIR"]
# poll intervals -- kepp within free limits -- 2000/month

CITIBIKE_INTERVAL = 2700
BUS_INTERVAL = 2700
SUBWAY_INTERVAL = 2700
TRAFFIC_INTERVAL = 2700

def get_connection():
    return sql.connect(
        server_hostname = DATABRICKS_HOST,
        http_path = DATABRICKS_HTTP_PATH,
        access_token = DATABRICKS_TOKEN
    )

def write_to_databricks(df: pl.DataFrame, table: str):
    if not df.is_empty():
        rows = df.to_dicts()
        cols = list(rows[0].keys())
        col_names = ', '.join(cols)
        placeholders = ', '.join(['?' for _ in cols])
        batch_size = 500
        total = 0

        try:
            with get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(
                        f"INSERT INTO transit.{table}"
                        f"({col_names}) VALUES ({placeholders})",
                        [list(r.values()) for r in batch]
                    )
                    total += len(batch)
                return total
        except Exception as e:
            print(f"[{table}] write error: {e}")
            return 0
    else:
        print(f"[{table}] no data to write")
        return 0

def fetch_citibike():
    now = datetime.utcnow()
    status_r = requests.get(
        "https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_status.json",
        timeout=5
    ).json()

    info_r = requests.get(
        "https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_information.json",
        timeout=5
    ).json()

    status_rows = [{
        'station_id': str(s['station_id']),
        'bikes_available': int(s.get('num_bikes_available', 0)),
        'ebikes_available': int(s.get('num_ebikes_available', 0)),
        'docks_available': int(s.get('num_docks_available', 0)),
        'is_renting': int(s.get('is_renting', 0)),
        'is_returning': int(s.get('is_returning', 0)),
        'ingested_at': now.isoformat()
    } for s in status_r['data']['stations']]

    station_rows = [{
        'station_id': str(s.get('station_id', '')),
        'station_name': str(s.get('name', '')),
        'short_name': str(s.get('short_name', '')),
        'lat': float(s.get('lat', 0)),
        'lon': float(s.get('lon', 0)),
        'capacity': int(s.get('capacity', 0)),
        'ingested_at': now.isoformat()
    } for s in info_r['data']['stations']]

    return (
        pl.DataFrame(status_rows),
        pl.DataFrame(station_rows)
    ) 

def fetch_buses():
    now = datetime.utcnow()
    try: 
        resp = requests.get(
            "https://bustime.mta.info/api/siri/vehicle-monitoring.json",
            params={
                "key": MTA_BUS_KEY,
                "version": '2.0',
                "VehicleMonitoringDetailLevel": "minimum"
            },
            timeout=10
        ).json()

        delivery = (
            resp['Siri']['ServiceDelivery']['VehicleMonitoringDelivery']
        ) 
        if isinstance(delivery, list):
            delivery = delivery[0]
        activities = delivery.get('VehicleActivity', [])
        if isinstance(activities, dict):
            activities = [activities]
    except Exception as e:
        print(f"Bus fetch error: {e}")
        return pl.DataFrame()
    
    rows = []

    for activity in activities:
        try: 
            j = activity.get('MonitoredVehicleJourney', {})
            loc = j.get('VehicaleLocation', {})
            if not loc:
                continue

            lat = float(loc.get('Latitude', 0))
            lon = float(loc.get('Longitude', 0))

            if lat == 0 or lon == 0:
                continue

            line_name = (
                j['PublishedLineName'][0]
                if isinstance(j.get('PublishedLineName'), list)
                else j.get('PublishedLineName', '')
            )

            dest_name = (
                j['DestinationName'][0]
                if isinstance(j.get('DestinationName'), list)
                else j.get('DestinationName', '')
            )

            mc = j.get('MonitoredCall', {})
            caps = mc.get('Extensions', {}).get('Capacities', {})

            set_refs = ','.join([s.get('SituationSimpleRef', '')
            for s in j.get('SituationRef', [])])

            rows.append({
                'vehicle_ref': str(j.get('VehicleRef', '')),
                'line_name': str(line_name),
                'lat': lat,
                'lon': lon,
                'expected_arrival': str(mc.get('ExpectedArrivalTime', '')),
                'distance_from_stop': int(mc.get('DistanceFromStop', 0) or 0),
                'stops_away': int(mc.get('NumberOfStopsAway', 0) or 0),
                'passenger_count': int(caps.get('EstimatedPassengerCount', 0)or 0),
                'passenger_capacity': int(caps.get('EstimatedPassengerCapacity', 0) or 0),
                'ingested_at': now.isoformat()
            })
        except Exception as e:
            print(f"Bus activity error: {e}")
            continue
    
    return pl.DataFrame(rows)
    
def fetch_subway():
    now = datetime.utcnow()
    rows = []

    for line in FEED_REPRESENTATIVES:
        try:
            feed = NYCTFeed(line)
            trains = feed.trips()

            for train in trains:
                if not train.location:
                    continue

                next_arrival = ''

                if train.stop_time_updates:
                    next_stop  = train.stop_time_updates[0]
                    next_stop_name = str(next_stop.stop_name or '')
                    next_arrival = (
                        next_stop.arrival.isoformat()
                        if next_stop.arrival else ''
                    )
                rows.append(
                    {
                        'trip_id': str(train.trip_id or ''),
                        'route_id': str(train.route_id or ''),
                        'direction': str(train.direction or ''),
                        'location_stop': str(train.location or ''),
                        'location_status': str(train.location_status or ''),
                        'next_arrival': next_arrival,
                        'last_update': (
                            train.last_position_update.isoformat()
                            if train.last_position_update else ''
                        ), 
                        'ingested_at': now.isoformat()
                    }
                )
        except Exception as e:
            print(f"Subway fetch {line} line group error: {e}")
            continue
    return pl.DataFrame(rows) if rows else pl.DataFrame()

def fetch_traffic():
    now = datetime.utcnow()

    try:
        data = requests.get(
            "https://data.cityofnewyork.us/resource/i4gi-tjb9.json"
            "?$limit=1000&$order=data_as_of+DESC",
            timeout=5
        ).json()

    except Exception as e:
        print(f"Traffic fetch error: {e}")
        return pl.DataFrame()
    
    rows = []
    for seg in data:
        try:
            rows.append({
                'segment_id':     str(seg.get('id', '')),
                'speed':          float(seg.get('speed', 0) or 0),
                'travel_time':    int(float(seg.get('travel_time', 0) or 0)),
                'borough':        str(seg.get('borough', '')),
                'link_points':    str(seg.get('link_points', '')),
                'data_as_of':     str(seg.get('data_as_of', '')),
                'ingested_at':    now.isoformat()
            })
        except Exception as e:
            print(f"Traffic segment error: {e}")
            continue
        
        return pl.DataFrame(rows) if rows else pl.DataFrame()

# rolling deletion



def rolling_delete(days_to_keep: int=30):
    if datetime.utcnow().hour != 0:
        return

    cutoff = (
        datetime.utcnow() - timedelta(days = days_to_keep)
    ).isoformat()

    tables = [
        'citibike_status',
        'bus_positions',
        'subway_positions',
        'traffic_speeds'
    ]

    print(f"\n  Rolling delete — removing rows before {cutoff}")

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                for table in tables:
                    cursor.execute(
                        f"""
                        DELETE FROM transit.{table} WHERE ingested_at < '{cutoff}'
                        """
                    )
                    print(f"Deleted old rows from {table}")

    except Exception as e:
        print(f"Error during rolling delete: {e}")

def check_row_counts():
    tables = [
        'citibike_status',
        'citibike_stations',
        'bus_positions',
        'subway_positions',
        'traffic_speeds'
    ]

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                for table in tables:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM transit.{table}"
                    )
                    count = cursor.fetchone()[0]
                    print(f"{table}: {count} rows")

    except Exception as e:
        print(f"Error during row count check: {e}")



if __name__ == "__main__":
    start = datetime.utcnow()
    print(f"\n{'='*50}")
    print(f"Poll run: {start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"{'='*50}")

    print("\nFetching Citibike data...")
    status_df, station_df = fetch_citibike()
    n = write_to_databricks(status_df, 'citibike_status')
    print(f"citibike status {n:>5} rows written")
    n = write_to_databricks(station_df, 'citibike_stations')
    print(f"citibike stations {n:>5} rows written")
    print("\nFetching MTA Bus data...")
    n = write_to_databricks(fetch_buses(), 'bus_positions')
    print(f"bus positions {n:>5} rows written")
    print("\nFetching Subway data...")
    n = write_to_databricks(fetch_subway(), 'subway_positions')
    print(f"subway positions {n:>5} rows written")
    print("\nFetching Traffic data...")
    n = write_to_databricks(fetch_traffic(), 'traffic_speeds')
    print(f"traffic speeds {n:>5} rows written")

    rolling_delete(days_to_keep=30)

    print("\nChecking row counts...")
    check_row_counts()

    elapsed = datetime.utcnow() - start
    print(f"\nCompleted in {elapsed}s")
    print(f"\n{'='*50}\n")









