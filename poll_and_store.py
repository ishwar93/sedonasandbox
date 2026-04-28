
import os
import sys
import requests
import polars as pl
from databricks import sql
from datetime import datetime, timedelta
from dotenv import load_dotenv
from nyct_gtfs import NYCTFeed
from zoneinfo import ZoneInfo

load_dotenv()

DATABRICKS_HOST      = os.environ['DATABRICKS_HOST']
DATABRICKS_HTTP_PATH = os.environ['DATABRICKS_HTTP_PATH']
DATABRICKS_TOKEN     = os.environ['DATABRICKS_TOKEN']
MTA_BUS_KEY          = os.environ['MTA_BUS_KEY']

# One representative per feed group — no API key required since MTA opened access.
FEED_REPRESENTATIVES = ["1", "A", "B", "G", "J", "N", "L", "SIR"]

ALERT_FEEDS = {
    'subway': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts.json',
    'bus':    'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fbus-alerts.json',
}

ALERT_TYPE_PRIORITY = {
    'Suspended':              35,
    'Severe Delays':          29,
    'Delays':                 26,
    'Expect Delays':          12,
    'Boarding Change':        10,
    'Reroute':                10,
    'Special Schedule':        5,
    'Detour':                  5,
    'No Scheduled Service':    1,
    'Information':             1,
}

NY_TZ = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")


def within_nyc_poll_window(now_utc: datetime | None = None) -> bool:
    """
    Return True only for the allowed NYC local poll window:
    hourly at :45 between 06:00 and 23:59 America/New_York.
    """
    now_utc = now_utc or datetime.utcnow().replace(tzinfo=UTC_TZ)
    ny_now = now_utc.astimezone(NY_TZ)
    return ny_now.minute == 45 and 6 <= ny_now.hour <= 23

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
    batch_size = 500
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
            timeout=15
        ).json()
        info_r = requests.get(
            "https://gbfs.lyft.com/gbfs/1.1/bkn/en/station_information.json",
            timeout=15
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
            timeout=60
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
            timeout=15
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

# ══════════════════════════════════════════════════════════════════════════════
# ADD THESE CONSTANTS near the top of poll_and_store.py
# alongside FEED_REPRESENTATIVES
# ══════════════════════════════════════════════════════════════════════════════

ALERT_FEEDS = {
    'subway': 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts.json',
    'bus':    'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fbus-alerts.json',
}

# priority_level derived from alert_type — entity selectors carry only sort_order,
# not a priority field. These values map to Mercury's priority enum used for
# PageRank edge weight reduction.
ALERT_TYPE_PRIORITY = {
    'Suspended':            35,
    'Severe Delays':        29,
    'Delays':               26,
    'Expect Delays':        12,
    'Boarding Change':      10,
    'Reroute':              10,
    'Special Schedule':      5,
    'Detour':                5,
    'No Scheduled Service':  1,
    'Information':           1,
}


# ══════════════════════════════════════════════════════════════════════════════
# ADD THESE FUNCTIONS before the if __name__ == '__main__': block
# ══════════════════════════════════════════════════════════════════════════════

def upsert_alerts(alerts: list[dict], periods: list[dict], entities: list[dict]) -> int:
    """
    Batch upsert service alerts using MERGE with UNION ALL source.
    Replaces active_periods and affected_entities with DELETE + INSERT.

    MERGE batches 100 alerts per statement (~2-3 round trips total)
    instead of one MERGE per alert (~200 round trips).

    Parameter count: 100 rows x 10 cols = 1,000 per MERGE — well under
    the Databricks 10,000 parameter limit.
    """
    if not alerts:
        return 0

    now        = datetime.utcnow().isoformat()
    MERGE_BATCH = 100

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:

                # ── Batched MERGE for service_alerts ──────────────────────────
                # USING clause built from UNION ALL of SELECT literals.
                # now is interpolated as a string literal (same value for all
                # rows) to avoid inflating the parameter count.
                for i in range(0, len(alerts), MERGE_BATCH):
                    batch = alerts[i:i + MERGE_BATCH]

                    union_rows = ' UNION ALL '.join(
                        'SELECT ? AS alert_id, ? AS feed_source, ? AS agency_id, '
                        '? AS header_text_plain, ? AS header_text_html, '
                        '? AS mercury_alert_type, ? AS mercury_updated_at, '
                        '? AS human_readable_period, ? AS is_planned, '
                        '? AS is_active_now'
                        for _ in batch
                    )

                    values = []
                    for a in batch:
                        values += [
                            a['alert_id'],
                            a['feed_source'],
                            a['agency_id'],
                            a['header_text_plain'],
                            a['header_text_html'],
                            a['mercury_alert_type'],
                            a['mercury_updated_at'],
                            a['human_readable_period'],
                            a['is_planned'],
                            a['is_active_now'],
                        ]

                    cursor.execute(f"""
                        MERGE INTO transit.service_alerts AS t
                        USING ({union_rows}) AS s
                        ON t.alert_id = s.alert_id
                        WHEN MATCHED THEN UPDATE SET
                            is_active_now         = s.is_active_now,
                            last_seen_at          = '{now}',
                            mercury_updated_at    = s.mercury_updated_at,
                            header_text_plain     = s.header_text_plain,
                            header_text_html      = s.header_text_html,
                            human_readable_period = s.human_readable_period,
                            ingested_at           = '{now}'
                        WHEN NOT MATCHED THEN INSERT (
                            alert_id, feed_source, agency_id,
                            header_text_plain, header_text_html,
                            mercury_alert_type, mercury_updated_at,
                            human_readable_period, is_planned,
                            is_active_now, first_seen_at,
                            last_seen_at, ingested_at
                        ) VALUES (
                            s.alert_id, s.feed_source, s.agency_id,
                            s.header_text_plain, s.header_text_html,
                            s.mercury_alert_type, s.mercury_updated_at,
                            s.human_readable_period, s.is_planned,
                            s.is_active_now, '{now}', '{now}', '{now}'
                        )
                    """, values)

                # ── DELETE existing periods + entities for all alerts ──────────
                # Both deletes share the same batched alert_id list.
                alert_ids = [a['alert_id'] for a in alerts]
                for i in range(0, len(alert_ids), 500):
                    batch        = alert_ids[i:i + 500]
                    placeholders = ', '.join(['?' for _ in batch])
                    cursor.execute(
                        f"DELETE FROM transit.service_alert_active_periods "
                        f"WHERE alert_id IN ({placeholders})", batch
                    )
                    cursor.execute(
                        f"DELETE FROM transit.service_alert_affected_entities "
                        f"WHERE alert_id IN ({placeholders})", batch
                    )

                # ── Batched INSERT for active_periods ─────────────────────────
                if periods:
                    rows = [
                        (p['alert_id'], p['period_seq'], p['starts_at'], p['ends_at'], now)
                        for p in periods
                    ]
                    for i in range(0, len(rows), 500):
                        batch        = rows[i:i + 500]
                        placeholders = ', '.join(['(?, ?, ?, ?, ?)' for _ in batch])
                        values       = [v for row in batch for v in row]
                        cursor.execute(
                            f"INSERT INTO transit.service_alert_active_periods "
                            f"(alert_id, period_seq, starts_at, ends_at, ingested_at) "
                            f"VALUES {placeholders}", values
                        )

                # ── Batched INSERT for affected_entities ──────────────────────
                if entities:
                    rows = [
                        (e['alert_id'], e['agency_id'], e['route_id'],
                         e['stop_id'], e['priority_level'], now)
                        for e in entities
                    ]
                    for i in range(0, len(rows), 500):
                        batch        = rows[i:i + 500]
                        placeholders = ', '.join(['(?, ?, ?, ?, ?, ?)' for _ in batch])
                        values       = [v for row in batch for v in row]
                        cursor.execute(
                            f"INSERT INTO transit.service_alert_affected_entities "
                            f"(alert_id, agency_id, route_id, stop_id, priority_level, ingested_at) "
                            f"VALUES {placeholders}", values
                        )

        return len(alerts)
    except Exception as e:
        print(f"  [service_alerts] write error: {e}")
        return 0


def fetch_alerts() -> tuple[list[dict], list[dict], list[dict]]:
    """
    Fetch subway and bus service alerts from MTA JSON feeds.
    No API key required as of 2026.

    JSON structure (real response verified):
      { "entity": [ { "id": "lmm:alert:NNN", "alert": { ... } }, ... ] }

    Key field notes:
      - is_planned:  derived from id prefix (lmm:planned_work: = planned)
      - priority_level: derived from mercury alert_type string — entity selectors
                        carry only sort_order, NOT a priority field
      - human_readable_active_period: single object {translation:[...]},
                        NOT a list — unlike active_period which IS a list
      - ends_at:     absent on open-ended (ongoing) alerts — stored as NULL

    Returns three lists: alerts, active_periods, affected_entities
    """
    now    = datetime.utcnow()
    now_ts = now.timestamp()
    alerts, periods, entities = [], [], []

    for feed_source, url in ALERT_FEEDS.items():
        try:
            resp = requests.get(url, timeout=15).json()
        except Exception as e:
            print(f"  [alerts:{feed_source}] fetch error: {e}")
            continue

        for entity in resp.get('entity', []):
            raw      = entity.get('alert', {})
            alert_id = str(entity.get('id', ''))
            if not alert_id:
                continue

            # ── is_planned: id prefix, not a feed field ───────────────────────
            # lmm:alert:NNN        = unplanned real-time disruption
            # lmm:planned_work:NNN = scheduled planned work
            is_planned = alert_id.startswith('lmm:planned_work:')

            # ── Header text ───────────────────────────────────────────────────
            header_trans = raw.get('header_text', {}).get('translation', [])
            header_plain = next(
                (t['text'] for t in header_trans if t.get('language') == 'en'),
                next((t['text'] for t in header_trans), '')
            )
            header_html = next(
                (t['text'] for t in header_trans if t.get('language') == 'en-html'),
                ''
            )

            # ── Mercury alert fields ──────────────────────────────────────────
            mercury    = raw.get('transit_realtime.mercury_alert', {})
            alert_type = str(mercury.get('alert_type', ''))
            updated_at = int(mercury.get('updated_at', 0) or 0)

            # human_readable_active_period is a SINGLE object {translation:[...]}
            # not a list — do not iterate it as a list
            hrp          = mercury.get('human_readable_active_period', {})
            hrp_trans    = hrp.get('translation', [])
            human_readable = next(
                (t['text'] for t in hrp_trans if t.get('language') == 'en'),
                next((t['text'] for t in hrp_trans), '')
            )

            # ── Active periods ────────────────────────────────────────────────
            is_active = False
            for seq, ap in enumerate(raw.get('active_period', [])):
                start = ap.get('start', 0)
                end   = ap.get('end')              # None = open-ended / ongoing
                periods.append({
                    'alert_id':   alert_id,
                    'period_seq': seq,
                    'starts_at':  datetime.utcfromtimestamp(start).isoformat() if start else None,
                    'ends_at':    datetime.utcfromtimestamp(end).isoformat()   if end   else None,
                })
                # is_active_now: any period that contains right now
                if start <= now_ts <= (end if end else float('inf')):
                    is_active = True

            # ── Affected entities ─────────────────────────────────────────────
            # priority_level comes from alert_type — entity selectors only carry
            # sort_order. Stop entities have no mercury_entity_selector at all.
            priority  = ALERT_TYPE_PRIORITY.get(alert_type, 0)
            agency_id = ''
            for ie in raw.get('informed_entity', []):
                a_id     = str(ie.get('agency_id', ''))
                route_id = str(ie.get('route_id', ''))
                stop_id  = str(ie.get('stop_id', ''))
                if a_id and not agency_id:
                    agency_id = a_id
                entities.append({
                    'alert_id':      alert_id,
                    'agency_id':     a_id,
                    'route_id':      route_id,
                    'stop_id':       stop_id,
                    'priority_level': priority,
                })

            alerts.append({
                'alert_id':             alert_id,
                'feed_source':          feed_source,
                'agency_id':            agency_id,
                'header_text_plain':    header_plain,
                'header_text_html':     header_html,
                'mercury_alert_type':   alert_type,
                'mercury_updated_at':   updated_at,
                'human_readable_period': human_readable,
                'is_planned':           is_planned,
                'is_active_now':        is_active,
            })

    return alerts, periods, entities





# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # if not within_nyc_poll_window():
    #     ny_now = datetime.utcnow().replace(tzinfo=UTC_TZ).astimezone(NY_TZ)
    #     print(
    #         "Skipping poll: outside NYC window "
    #         f"(local time {ny_now.strftime('%Y-%m-%d %H:%M:%S %Z')})"
    #     )
    #     sys.exit(0)

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


    print("\nFetching MTA Service Alerts...")
    alerts, periods, entities = fetch_alerts()
    n = upsert_alerts(alerts, periods, entities)
    print(f"  service_alerts:    {n:>5} alerts upserted")
    print(f"  active_periods:    {len(periods):>5} periods written")
    print(f"  affected_entities: {len(entities):>5} entities written")