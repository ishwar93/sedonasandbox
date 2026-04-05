import os
from databricks import sql
from datetime import datetime, timedelta
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


def already_done(table: str, date_col: str, date_val: str) -> bool:
    """Check if a date has already been aggregated — makes every function idempotent."""
    # try:
    #     with get_connection() as conn:
    #         with conn.cursor() as cursor:
    #             cursor.execute(f"""
    #                 SELECT COUNT(*) FROM transit.{table}
    #                 WHERE {date_col} = '{date_val}'
    #             """)
    #             return cursor.fetchone()[0] > 0
    # except Exception:
    #     return False
    return False


def run_sql(label: str, sql_str: str):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_str)
        print(f"  ✓ {label}")
    except Exception as e:
        print(f"  ✗ {label}: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# DAILY AGGREGATIONS — runs every day at 23:00 UTC
# Summarises yesterday's raw rows into hourly and daily agg tables.
# ══════════════════════════════════════════════════════════════════════════════

def aggregate_traffic_hourly(date: str):
    if already_done('agg_traffic_hourly', 'DATE(hour_bucket)', date):
        print(f"  [agg_traffic_hourly] {date} already done — skipping")
        return
    run_sql(f"agg_traffic_hourly {date}", f"""
        INSERT INTO transit.agg_traffic_hourly
        SELECT
            segment_id,
            borough,
            DATE_TRUNC('hour', ingested_at)         AS hour_bucket,
            YEAR(ingested_at)                       AS data_year,
            MONTH(ingested_at)                      AS month_of_year,
            WEEKOFYEAR(ingested_at)                 AS week_of_year,
            ((DAYOFMONTH(ingested_at) - 1) / 7 + 1) AS week_of_month,
            DAYOFWEEK(ingested_at)                  AS day_of_week,
            HOUR(ingested_at)                       AS hour_of_day,
            AVG(speed)                              AS avg_speed,
            MIN(speed)                              AS min_speed,
            MAX(speed)                              AS max_speed,
            STDDEV(speed)                           AS stddev_speed,
            AVG(travel_time)                        AS avg_travel_time,
            COUNT(*)                                AS sample_count
        FROM transit.traffic_speeds
        WHERE DATE(ingested_at) = '{date}'
        GROUP BY
            segment_id, borough,
            DATE_TRUNC('hour', ingested_at),
            YEAR(ingested_at), MONTH(ingested_at),
            WEEKOFYEAR(ingested_at),
            ((DAYOFMONTH(ingested_at) - 1) / 7 + 1),
            DAYOFWEEK(ingested_at),
            HOUR(ingested_at)
    """)


def aggregate_traffic_daily(date: str):
    if already_done('agg_traffic_daily', 'calendar_date', date):
        print(f"  [agg_traffic_daily] {date} already done — skipping")
        return
    run_sql(f"agg_traffic_daily {date}", f"""
        INSERT INTO transit.agg_traffic_daily
        SELECT
            segment_id,
            borough,
            '{date}'                                AS calendar_date,
            YEAR(ingested_at)                       AS data_year,
            MONTH(ingested_at)                      AS month_of_year,
            WEEKOFYEAR(ingested_at)                 AS week_of_year,
            ((DAYOFMONTH(ingested_at) - 1) / 7 + 1) AS week_of_month,
            DAYOFWEEK(ingested_at)                  AS day_of_week,
            AVG(speed)                              AS avg_speed,
            MIN(speed)                              AS min_speed,
            MAX(speed)                              AS max_speed,
            STDDEV(speed)                           AS stddev_speed,
            AVG(travel_time)                        AS avg_travel_time,
            AVG(CASE WHEN HOUR(ingested_at) BETWEEN 7  AND 9  THEN speed END) AS avg_speed_am_peak,
            AVG(CASE WHEN HOUR(ingested_at) BETWEEN 17 AND 19 THEN speed END) AS avg_speed_pm_peak,
            AVG(CASE WHEN HOUR(ingested_at) >= 22
                      OR HOUR(ingested_at) <= 5    THEN speed END) AS avg_speed_overnight,
            NULL                                    AS worst_hour,
            NULL                                    AS best_hour,
            COUNT(*)                                AS sample_count
        FROM transit.traffic_speeds
        WHERE DATE(ingested_at) = '{date}'
        GROUP BY
            segment_id, borough,
            YEAR(ingested_at), MONTH(ingested_at),
            WEEKOFYEAR(ingested_at),
            ((DAYOFMONTH(ingested_at) - 1) / 7 + 1),
            DAYOFWEEK(ingested_at)
    """)


def aggregate_citibike_hourly(date: str):
    if already_done('agg_citibike_hourly', 'DATE(hour_bucket)', date):
        print(f"  [agg_citibike_hourly] {date} already done — skipping")
        return
    run_sql(f"agg_citibike_hourly {date}", f"""
        INSERT INTO transit.agg_citibike_hourly
        SELECT
            station_id,
            DATE_TRUNC('hour', ingested_at)         AS hour_bucket,
            YEAR(ingested_at)                       AS data_year,
            MONTH(ingested_at)                      AS month_of_year,
            WEEKOFYEAR(ingested_at)                 AS week_of_year,
            ((DAYOFMONTH(ingested_at) - 1) / 7 + 1) AS week_of_month,
            DAYOFWEEK(ingested_at)                  AS day_of_week,
            HOUR(ingested_at)                       AS hour_of_day,
            CONCAT(YEAR(ingested_at), '-', LPAD(MONTH(ingested_at), 2, '0')) AS month_year,
            AVG(bikes_available)                    AS avg_bikes,
            MIN(bikes_available)                    AS min_bikes,
            MAX(bikes_available)                    AS max_bikes,
            AVG(ebikes_available)                   AS avg_ebikes,
            AVG(docks_available)                    AS avg_docks,
            AVG(CASE WHEN bikes_available = 0 THEN 1.0 ELSE 0.0 END) AS pct_time_empty,
            AVG(CASE WHEN docks_available = 0 THEN 1.0 ELSE 0.0 END) AS pct_time_full,
            COUNT(*)                                AS sample_count
        FROM transit.citibike_status
        WHERE DATE(ingested_at) = '{date}'
        GROUP BY
            station_id,
            DATE_TRUNC('hour', ingested_at),
            YEAR(ingested_at), MONTH(ingested_at),
            WEEKOFYEAR(ingested_at),
            ((DAYOFMONTH(ingested_at) - 1) / 7 + 1),
            DAYOFWEEK(ingested_at),
            HOUR(ingested_at)
    """)


def aggregate_citibike_daily(date: str):
    if already_done('agg_citibike_daily', 'calendar_date', date):
        print(f"  [agg_citibike_daily] {date} already done — skipping")
        return
    run_sql(f"agg_citibike_daily {date}", f"""
        INSERT INTO transit.agg_citibike_daily
        SELECT
            station_id,
            '{date}'                                AS calendar_date,
            YEAR(ingested_at)                       AS data_year,
            MONTH(ingested_at)                      AS month_of_year,
            WEEKOFYEAR(ingested_at)                 AS week_of_year,
            ((DAYOFMONTH(ingested_at) - 1) / 7 + 1) AS week_of_month,
            DAYOFWEEK(ingested_at)                  AS day_of_week,
            CONCAT(YEAR(ingested_at), '-', LPAD(MONTH(ingested_at), 2, '0')) AS month_year,
            AVG(bikes_available)                    AS avg_bikes,
            AVG(ebikes_available)                   AS avg_ebikes,
            AVG(docks_available)                    AS avg_docks,
            AVG(CASE WHEN bikes_available = 0 THEN 1.0 ELSE 0.0 END) AS pct_time_empty,
            AVG(CASE WHEN docks_available = 0 THEN 1.0 ELSE 0.0 END) AS pct_time_full,
            1.0 - AVG(CASE WHEN bikes_available = 0 THEN 1.0 ELSE 0.0 END) AS reliability_score,
            AVG(CASE WHEN HOUR(ingested_at) BETWEEN 7  AND 9  AND bikes_available = 0 THEN 1.0 ELSE 0.0 END) AS pct_empty_am_peak,
            AVG(CASE WHEN HOUR(ingested_at) BETWEEN 17 AND 19 AND bikes_available = 0 THEN 1.0 ELSE 0.0 END) AS pct_empty_pm_peak,
            AVG(CASE WHEN HOUR(ingested_at) >= 22
                      OR  HOUR(ingested_at) <= 5   AND bikes_available = 0 THEN 1.0 ELSE 0.0 END) AS pct_empty_overnight,
            NULL                                    AS worst_hour,
            NULL                                    AS best_hour,
            COUNT(*)                                AS sample_count
        FROM transit.citibike_status
        WHERE DATE(ingested_at) = '{date}'
        GROUP BY
            station_id,
            YEAR(ingested_at), MONTH(ingested_at),
            WEEKOFYEAR(ingested_at),
            ((DAYOFMONTH(ingested_at) - 1) / 7 + 1),
            DAYOFWEEK(ingested_at)
    """)


def aggregate_bus_hourly(date: str):
    if already_done('agg_bus_hourly', 'DATE(hour_bucket)', date):
        print(f"  [agg_bus_hourly] {date} already done — skipping")
        return
    run_sql(f"agg_bus_hourly {date}", f"""
        INSERT INTO transit.agg_bus_hourly
        SELECT
            line_name,
            DATE_TRUNC('hour', ingested_at)         AS hour_bucket,
            YEAR(ingested_at)                       AS data_year,
            MONTH(ingested_at)                      AS month_of_year,
            WEEKOFYEAR(ingested_at)                 AS week_of_year,
            ((DAYOFMONTH(ingested_at) - 1) / 7 + 1) AS week_of_month,
            DAYOFWEEK(ingested_at)                  AS day_of_week,
            HOUR(ingested_at)                       AS hour_of_day,
            CONCAT(YEAR(ingested_at), '-', LPAD(MONTH(ingested_at), 2, '0')) AS month_year,
            COUNT(DISTINCT vehicle_ref)             AS unique_vehicles,
            AVG(CASE WHEN passenger_capacity > 0
                AND  CAST(passenger_count AS DOUBLE) / passenger_capacity >= 0.9
                THEN 1.0 ELSE 0.0 END)              AS pct_full,
            AVG(passenger_count)                    AS avg_passenger_count,
            AVG(CASE WHEN passenger_capacity > 0
                THEN CAST(passenger_count AS DOUBLE) / passenger_capacity
                ELSE NULL END)                      AS avg_passenger_load,
            NULL                                    AS unique_destinations,

            COUNT(*)                                AS sample_count
        FROM transit.bus_positions
        WHERE DATE(ingested_at) = '{date}'
        GROUP BY
            line_name,
            DATE_TRUNC('hour', ingested_at),
            YEAR(ingested_at), MONTH(ingested_at),
            WEEKOFYEAR(ingested_at),
            ((DAYOFMONTH(ingested_at) - 1) / 7 + 1),
            DAYOFWEEK(ingested_at),
            HOUR(ingested_at)
    """)


def aggregate_bus_daily(date: str):
    if already_done('agg_bus_daily', 'calendar_date', date):
        print(f"  [agg_bus_daily] {date} already done — skipping")
        return
    run_sql(f"agg_bus_daily {date}", f"""
        INSERT INTO transit.agg_bus_daily
        SELECT
            line_name,
            '{date}'                                AS calendar_date,
            YEAR(ingested_at)                       AS data_year,
            MONTH(ingested_at)                      AS month_of_year,
            WEEKOFYEAR(ingested_at)                 AS week_of_year,
            ((DAYOFMONTH(ingested_at) - 1) / 7 + 1) AS week_of_month,
            DAYOFWEEK(ingested_at)                  AS day_of_week,
            CONCAT(YEAR(ingested_at), '-', LPAD(MONTH(ingested_at), 2, '0')) AS month_year,
            COUNT(DISTINCT vehicle_ref)             AS avg_vehicles,
            AVG(CASE WHEN passenger_capacity > 0
                AND  CAST(passenger_count AS DOUBLE) / passenger_capacity >= 0.9
                THEN 1.0 ELSE 0.0 END)              AS avg_pct_full,
            AVG(CASE WHEN passenger_capacity > 0
                THEN CAST(passenger_count AS DOUBLE) / passenger_capacity
                ELSE NULL END)                      AS avg_passenger_load,
            AVG(passenger_count)                    AS avg_passenger_count,
            AVG(CASE WHEN HOUR(ingested_at) BETWEEN 7  AND 9  AND passenger_capacity > 0
                THEN CAST(passenger_count AS DOUBLE) / passenger_capacity END) AS avg_load_am_peak,
            AVG(CASE WHEN HOUR(ingested_at) BETWEEN 17 AND 19 AND passenger_capacity > 0
                THEN CAST(passenger_count AS DOUBLE) / passenger_capacity END) AS avg_load_pm_peak,
            AVG(CASE WHEN (HOUR(ingested_at) >= 22 OR HOUR(ingested_at) <= 5)
                AND passenger_capacity > 0
                THEN CAST(passenger_count AS DOUBLE) / passenger_capacity END) AS avg_load_overnight,
            MAX(CASE WHEN passenger_capacity > 0
                THEN CAST(passenger_count AS DOUBLE) / passenger_capacity END) AS peak_vehicles,
            NULL                                    AS worst_hour,
            NULL                                    AS best_hour,
            COUNT(*)                                AS sample_count
        FROM transit.bus_positions
        WHERE DATE(ingested_at) = '{date}'
        GROUP BY
            line_name,
            YEAR(ingested_at), MONTH(ingested_at),
            WEEKOFYEAR(ingested_at),
            ((DAYOFMONTH(ingested_at) - 1) / 7 + 1),
            DAYOFWEEK(ingested_at)
    """)


# ══════════════════════════════════════════════════════════════════════════════
# WEEKLY AGGREGATIONS — runs on Sunday at 23:00 UTC
# Reads from hourly agg tables — never touches raw data again.
# Week is qualified by month_of_year so "January week 2" is unambiguous.
# ══════════════════════════════════════════════════════════════════════════════

def aggregate_traffic_weekly(week_of_year: int, week_of_month: int, month: int, data_year: int):
    key = f"{data_year}-{month:02d}-w{week_of_month}"
    if already_done('agg_traffic_weekly', 'week_start_date', key):
        print(f"  [agg_traffic_weekly] {key} already done — skipping")
        return
    run_sql(f"agg_traffic_weekly {key}", f"""
        INSERT INTO transit.agg_traffic_weekly
        SELECT
            segment_id,
            borough,
            {data_year}         AS data_year,
            {month}             AS month_of_year,
            {week_of_year}      AS week_of_year,
            {week_of_month}     AS week_of_month,
            '{key}'             AS week_start_date,
            AVG(avg_speed)      AS avg_speed,
            MIN(min_speed)      AS min_speed,
            MAX(max_speed)      AS max_speed,
            AVG(stddev_speed)   AS stddev_speed,
            AVG(avg_travel_time) AS avg_travel_time,
            AVG(CASE WHEN day_of_week = 2 THEN avg_speed END) AS avg_speed_mon,
            AVG(CASE WHEN day_of_week = 3 THEN avg_speed END) AS avg_speed_tue,
            AVG(CASE WHEN day_of_week = 4 THEN avg_speed END) AS avg_speed_wed,
            AVG(CASE WHEN day_of_week = 5 THEN avg_speed END) AS avg_speed_thu,
            AVG(CASE WHEN day_of_week = 6 THEN avg_speed END) AS avg_speed_fri,
            AVG(CASE WHEN day_of_week = 7 THEN avg_speed END) AS avg_speed_sat,
            AVG(CASE WHEN day_of_week = 1 THEN avg_speed END) AS avg_speed_sun,
            AVG(CASE WHEN hour_of_day BETWEEN 7  AND 19 THEN avg_speed END) AS avg_peak_speed,
            AVG(CASE WHEN hour_of_day < 7 OR hour_of_day > 19 THEN avg_speed END) AS avg_offpeak_speed,
            NULL AS worst_day,
            NULL AS best_day,
            NULL AS worst_hour,
            NULL AS best_hour,
            SUM(sample_count)   AS sample_count
        FROM transit.agg_traffic_hourly
        WHERE data_year = {data_year}
        AND   month_of_year = {month}
        AND   week_of_month = {week_of_month}
        GROUP BY segment_id, borough
    """)


def aggregate_citibike_weekly(week_of_year: int, week_of_month: int, month: int, data_year: int):
    key = f"{data_year}-{month:02d}-w{week_of_month}"
    if already_done('agg_citibike_weekly', 'week_start_date', key):
        print(f"  [agg_citibike_weekly] {key} already done — skipping")
        return
    run_sql(f"agg_citibike_weekly {key}", f"""
        INSERT INTO transit.agg_citibike_weekly
        SELECT
            station_id,
            {data_year}         AS data_year,
            {month}             AS month_of_year,
            {week_of_year}      AS week_of_year,
            {week_of_month}     AS week_of_month,
            '{key}'             AS week_start_date,
            CONCAT({data_year}, '-', LPAD({month}, 2, '0')) AS month_year,
            AVG(avg_bikes)      AS avg_bikes,
            AVG(avg_ebikes)     AS avg_ebikes,
            AVG(pct_time_empty) AS pct_time_empty,
            AVG(pct_time_full)  AS pct_time_full,
            1.0 - AVG(pct_time_empty) AS reliability_score,
            AVG(CASE WHEN day_of_week = 2 THEN pct_time_empty END) AS pct_empty_mon,
            AVG(CASE WHEN day_of_week = 3 THEN pct_time_empty END) AS pct_empty_tue,
            AVG(CASE WHEN day_of_week = 4 THEN pct_time_empty END) AS pct_empty_wed,
            AVG(CASE WHEN day_of_week = 5 THEN pct_time_empty END) AS pct_empty_thu,
            AVG(CASE WHEN day_of_week = 6 THEN pct_time_empty END) AS pct_empty_fri,
            AVG(CASE WHEN day_of_week = 7 THEN pct_time_empty END) AS pct_empty_sat,
            AVG(CASE WHEN day_of_week = 1 THEN pct_time_empty END) AS pct_empty_sun,
            NULL                                    AS worst_day,
            NULL                                    AS best_day,
            NULL                                    AS worst_hour,
            NULL                                    AS best_hour,
            SUM(sample_count)   AS sample_count
        FROM transit.agg_citibike_hourly
        WHERE data_year = {data_year}
        AND   month_of_year = {month}
        AND   week_of_month = {week_of_month}
        GROUP BY station_id
    """)
def aggregate_citibike_monthly(month: int, data_year: int):
    key = f"{data_year}-{month:02d}"
    if already_done('agg_citibike_monthly', 'month_year', key):
        print(f"  [agg_citibike_monthly] {key} already done — skipping")
        return
    run_sql(f"agg_citibike_monthly {key}", f"""
        INSERT INTO transit.agg_citibike_monthly
        SELECT
            station_id,
            {month}             AS month_of_year,
            {data_year}         AS data_year,
            CONCAT({data_year}, '-', LPAD({month}, 2, '0')) AS month_year,
            AVG(avg_bikes)      AS avg_bikes,
            AVG(avg_ebikes)     AS avg_ebikes,
            AVG(avg_docks)      AS avg_docks,
            AVG(pct_time_empty) AS pct_time_empty,
            AVG(pct_time_full)  AS pct_time_full,
            1.0 - AVG(pct_time_empty) AS reliability_score,
            SUM(sample_count)   AS sample_count
        FROM transit.agg_citibike_weekly
        WHERE data_year = {data_year}
        AND   month_of_year = {month}
        GROUP BY station_id
    """)


def aggregate_bus_weekly(week_of_year: int, week_of_month: int, month: int, data_year: int):
    key = f"{data_year}-{month:02d}-w{week_of_month}"
    if already_done('agg_bus_weekly', 'week_start_date', key):
        print(f"  [agg_bus_weekly] {key} already done — skipping")
        return
    run_sql(f"agg_bus_weekly {key}", f"""
        INSERT INTO transit.agg_bus_weekly
        SELECT
            line_name,
            {data_year}             AS data_year,
            {month}                 AS month_of_year,
            {week_of_year}          AS week_of_year,
            {week_of_month}         AS week_of_month,
            '{key}'                 AS week_start_date,
            CONCAT({data_year}, '-', LPAD({month}, 2, '0')) AS month_year,
            AVG(unique_vehicles)    AS avg_vehicles,
            AVG(pct_full)           AS avg_pct_full,
            AVG(avg_passenger_load) AS avg_passenger_load,
            AVG(CASE WHEN day_of_week = 2 THEN avg_passenger_load END) AS avg_load_mon,
            AVG(CASE WHEN day_of_week = 3 THEN avg_passenger_load END) AS avg_load_tue,
            AVG(CASE WHEN day_of_week = 4 THEN avg_passenger_load END) AS avg_load_wed,
            AVG(CASE WHEN day_of_week = 5 THEN avg_passenger_load END) AS avg_load_thu,
            AVG(CASE WHEN day_of_week = 6 THEN avg_passenger_load END) AS avg_load_fri,
            AVG(CASE WHEN day_of_week = 7 THEN avg_passenger_load END) AS avg_load_sat,
            AVG(CASE WHEN day_of_week = 1 THEN avg_passenger_load END) AS avg_load_sun,
            AVG(CASE WHEN hour_of_day BETWEEN 7  AND 19 THEN avg_passenger_load END) AS avg_load_peak,
            AVG(CASE WHEN hour_of_day < 7 OR hour_of_day > 19 THEN avg_passenger_load END) AS avg_load_offpeak,
            NULL                                    AS worst_day,
            NULL                                    AS best_day,
            SUM(sample_count)       AS sample_count
        FROM transit.agg_bus_hourly
        WHERE data_year = {data_year}
        AND   month_of_year = {month}
        AND   week_of_month = {week_of_month}
        GROUP BY line_name
    """)


def aggregate_bus_monthly(month: int, data_year: int):
    key = f"{data_year}-{month:02d}"
    if already_done('agg_bus_monthly', 'month_year', key):
        print(f"  [agg_bus_monthly] {key} already done — skipping")
        return
    run_sql(f"agg_bus_monthly {key}", f"""
        INSERT INTO transit.agg_bus_monthly
        SELECT
            line_name,
            {month}                 AS month_of_year,
            {data_year}             AS data_year,
            CONCAT({data_year}, '-', LPAD({month}, 2, '0')) AS month_year,
            AVG(avg_vehicles)       AS avg_vehicles,
            AVG(avg_pct_full)       AS avg_pct_full,
            AVG(avg_passenger_load) AS avg_passenger_load,
            NULL                    AS load_vs_prev_month,  -- computed separately if needed
            SUM(sample_count)       AS sample_count
        FROM transit.agg_bus_weekly
        WHERE data_year = {data_year}
        AND   month_of_year = {month}
        GROUP BY line_name
    """)

def aggregate_traffic_yearly(data_year: int):
    if already_done('agg_traffic_yearly', 'data_year', str(data_year)):
        print(f"  [agg_traffic_yearly] {data_year} already done — skipping")
        return
    run_sql(f"agg_traffic_yearly {data_year}", f"""
        INSERT INTO transit.agg_traffic_yearly
        SELECT
            m.segment_id,
            m.borough,
            {data_year}                                             AS data_year,
            AVG(m.avg_speed)                                        AS avg_speed,
            MIN(m.min_speed)                                        AS min_speed,
            MAX(m.max_speed)                                        AS max_speed,
            AVG(m.stddev_speed)                                     AS stddev_speed,
            AVG(m.avg_travel_time)                                  AS avg_travel_time,
            AVG(m.avg_peak_speed)                                   AS avg_peak_speed,
            AVG(m.avg_offpeak_speed)                                AS avg_offpeak_speed,
            AVG(CASE WHEN w.day_of_week = 2 THEN w.avg_speed END)  AS avg_speed_monday,
            AVG(CASE WHEN w.day_of_week = 6 THEN w.avg_speed END)  AS avg_speed_friday,
            AVG(CASE WHEN w.day_of_week = 7 THEN w.avg_speed END)  AS avg_speed_saturday,
            AVG(CASE WHEN w.day_of_week = 1 THEN w.avg_speed END)  AS avg_speed_sunday,
            AVG(CASE WHEN m.month_of_year IN (1,2,3)   THEN m.avg_speed END) AS avg_speed_q1,
            AVG(CASE WHEN m.month_of_year IN (4,5,6)   THEN m.avg_speed END) AS avg_speed_q2,
            AVG(CASE WHEN m.month_of_year IN (7,8,9)   THEN m.avg_speed END) AS avg_speed_q3,
            AVG(CASE WHEN m.month_of_year IN (10,11,12) THEN m.avg_speed END) AS avg_speed_q4,
            (SELECT month_of_year FROM transit.agg_traffic_monthly m2
             WHERE m2.segment_id = m.segment_id AND m2.data_year = {data_year}
             ORDER BY avg_speed ASC  LIMIT 1)                       AS worst_month,
            (SELECT month_of_year FROM transit.agg_traffic_monthly m2
             WHERE m2.segment_id = m.segment_id AND m2.data_year = {data_year}
             ORDER BY avg_speed DESC LIMIT 1)                       AS best_month,
            (SELECT hour_of_day FROM transit.agg_traffic_hourly h
             WHERE h.segment_id = m.segment_id AND h.data_year = {data_year}
             GROUP BY hour_of_day ORDER BY AVG(avg_speed) ASC  LIMIT 1) AS worst_hour,
            (SELECT hour_of_day FROM transit.agg_traffic_hourly h
             WHERE h.segment_id = m.segment_id AND h.data_year = {data_year}
             GROUP BY hour_of_day ORDER BY AVG(avg_speed) DESC LIMIT 1) AS best_hour,
            NULL                                                    AS yoy_speed_change,
            SUM(m.sample_count)                                     AS sample_count
        FROM transit.agg_traffic_monthly m
        LEFT JOIN transit.agg_traffic_weekly w
            ON  w.segment_id    = m.segment_id
            AND w.data_year     = m.data_year
        WHERE m.data_year = {data_year}
        GROUP BY m.segment_id, m.borough
    """)


def aggregate_citibike_yearly(data_year: int):
    if already_done('agg_citibike_yearly', 'data_year', str(data_year)):
        print(f"  [agg_citibike_yearly] {data_year} already done — skipping")
        return
    run_sql(f"agg_citibike_yearly {data_year}", f"""
        INSERT INTO transit.agg_citibike_yearly
        SELECT
            m.station_id,
            {data_year}                                                         AS data_year,
            AVG(m.avg_bikes)                                                    AS avg_bikes,
            AVG(m.avg_ebikes)                                                   AS avg_ebikes,
            AVG(m.avg_docks)                                                    AS avg_docks,
            AVG(m.pct_time_empty)                                               AS pct_time_empty,
            AVG(m.pct_time_full)                                                AS pct_time_full,
            1.0 - AVG(m.pct_time_empty)                                         AS reliability_score,
            AVG(CASE WHEN m.month_of_year IN (1,2,3)    THEN m.reliability_score END) AS reliability_q1,
            AVG(CASE WHEN m.month_of_year IN (4,5,6)    THEN m.reliability_score END) AS reliability_q2,
            AVG(CASE WHEN m.month_of_year IN (7,8,9)    THEN m.reliability_score END) AS reliability_q3,
            AVG(CASE WHEN m.month_of_year IN (10,11,12) THEN m.reliability_score END) AS reliability_q4,
            AVG(CASE WHEN w.day_of_week BETWEEN 2 AND 6 THEN w.pct_time_empty END) AS pct_empty_weekday,
            AVG(CASE WHEN w.day_of_week IN (1, 7)       THEN w.pct_time_empty END) AS pct_empty_weekend,
            AVG(h.pct_time_empty) FILTER (WHERE h.hour_of_day BETWEEN 7 AND 9)     AS pct_empty_am_peak,
            AVG(h.pct_time_empty) FILTER (WHERE h.hour_of_day BETWEEN 17 AND 19)   AS pct_empty_pm_peak,
            (SELECT month_of_year FROM transit.agg_citibike_monthly m2
             WHERE m2.station_id = m.station_id AND m2.data_year = {data_year}
             ORDER BY reliability_score ASC  LIMIT 1)               AS worst_month,
            (SELECT month_of_year FROM transit.agg_citibike_monthly m2
             WHERE m2.station_id = m.station_id AND m2.data_year = {data_year}
             ORDER BY reliability_score DESC LIMIT 1)               AS best_month,
            NULL                                                    AS yoy_reliability_change,
            SUM(m.sample_count)                                     AS sample_count
        FROM transit.agg_citibike_monthly m
        LEFT JOIN transit.agg_citibike_weekly w
            ON  w.station_id    = m.station_id
            AND w.data_year     = m.data_year
        LEFT JOIN transit.agg_citibike_hourly h
            ON  h.station_id    = m.station_id
            AND h.data_year     = m.data_year
        WHERE m.data_year = {data_year}
        GROUP BY m.station_id
    """)

def aggregate_bus_yearly(data_year: int):
    if already_done('agg_bus_yearly', 'data_year', str(data_year)):
        print(f"  [agg_bus_yearly] {data_year} already done — skipping")
        return
    run_sql(f"agg_bus_yearly {data_year}", f"""
        INSERT INTO transit.agg_bus_yearly
        SELECT
            m.line_name,
            {data_year}                                                         AS data_year,
            AVG(m.avg_vehicles)                                                 AS avg_vehicles,
            AVG(m.avg_pct_full)                                                 AS avg_pct_full,
            AVG(m.avg_passenger_load)                                           AS avg_passenger_load,
            AVG(h.avg_passenger_count)                                          AS avg_passenger_count,
            AVG(CASE WHEN m.month_of_year IN (1,2,3)    THEN m.avg_passenger_load END) AS avg_load_q1,
            AVG(CASE WHEN m.month_of_year IN (4,5,6)    THEN m.avg_passenger_load END) AS avg_load_q2,
            AVG(CASE WHEN m.month_of_year IN (7,8,9)    THEN m.avg_passenger_load END) AS avg_load_q3,
            AVG(CASE WHEN m.month_of_year IN (10,11,12) THEN m.avg_passenger_load END) AS avg_load_q4,
            AVG(h.avg_passenger_load) FILTER (WHERE h.hour_of_day BETWEEN 7  AND 9)  AS avg_load_am_peak,
            AVG(h.avg_passenger_load) FILTER (WHERE h.hour_of_day BETWEEN 17 AND 19) AS avg_load_pm_peak,
            AVG(h.avg_passenger_load) FILTER (WHERE h.hour_of_day < 7
                                               OR   h.hour_of_day > 19)              AS avg_load_offpeak,
            AVG(w.avg_passenger_load) FILTER (WHERE w.day_of_week BETWEEN 2 AND 6)   AS avg_load_weekday,
            AVG(w.avg_passenger_load) FILTER (WHERE w.day_of_week IN (1, 7))         AS avg_load_weekend,
            AVG(h.avg_passenger_load) FILTER (WHERE h.hour_of_day BETWEEN 7  AND 19) AS avg_vehicles_peak,
            AVG(h.avg_passenger_load) FILTER (WHERE h.hour_of_day < 7
                                               OR   h.hour_of_day > 19)              AS avg_vehicles_offpeak,
            (SELECT month_of_year FROM transit.agg_bus_monthly m2
             WHERE m2.line_name = m.line_name AND m2.data_year = {data_year}
             ORDER BY avg_passenger_load DESC LIMIT 1)              AS worst_month,
            (SELECT month_of_year FROM transit.agg_bus_monthly m2
             WHERE m2.line_name = m.line_name AND m2.data_year = {data_year}
             ORDER BY avg_passenger_load ASC  LIMIT 1)              AS best_month,
            NULL                                                    AS yoy_load_change,
            NULL                                                    AS yoy_frequency_change,
            SUM(m.sample_count)                                     AS sample_count
        FROM transit.agg_bus_monthly m
        LEFT JOIN transit.agg_bus_weekly w
            ON  w.line_name     = m.line_name
            AND w.data_year     = m.data_year
        LEFT JOIN transit.agg_bus_hourly h
            ON  h.line_name     = m.line_name
            AND h.data_year     = m.data_year
        WHERE m.data_year = {data_year}
        GROUP BY m.line_name
    """)


    
# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION — determines what to run based on current datetime
# ══════════════════════════════════════════════════════════════════════════════

def run_aggregations():
    now       = datetime.utcnow()
    yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    prev      = now - timedelta(days=1)

    print(f"\n{'='*50}")
    print(f"Aggregation run: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"Aggregating date: {yesterday}")
    print(f"{'='*50}")

    # ── Daily — runs every time ──────────────────────────────────────────────
    print("\nDaily aggregations (hourly + daily)...")
    aggregate_traffic_hourly(yesterday)
    aggregate_traffic_daily(yesterday)
    aggregate_citibike_hourly(yesterday)
    aggregate_citibike_daily(yesterday)
    aggregate_bus_hourly(yesterday)
    aggregate_bus_daily(yesterday)

    # ── Weekly — runs on Sunday ──────────────────────────────────────────────
    # now.weekday() == 6 means today is Sunday — yesterday completed the week
    if now.weekday() == 6:
        print("\nWeekly aggregations (Sunday run)...")
        woy = int(prev.strftime('%W'))           # week of year
        wom = ((prev.day - 1) // 7) + 1         # week of month (1-5)
        aggregate_traffic_weekly(woy, wom, prev.month, prev.year)
        aggregate_citibike_weekly(woy, wom, prev.month, prev.year)
        aggregate_bus_weekly(woy, wom, prev.month, prev.year)

    # ── Monthly — runs on 1st of month ──────────────────────────────────────
    # now.day == 1 means yesterday was the last day of the previous month
    if now.day == 1:
        print("\nMonthly aggregations (1st of month run)...")
        aggregate_traffic_monthly(prev.month, prev.year)
        aggregate_citibike_monthly(prev.month, prev.year)
        aggregate_bus_monthly(prev.month, prev.year)

    print(f"\nCompleted in {(datetime.utcnow() - now).seconds}s")
    print(f"{'='*50}\n")

    # ── Yearly — runs on Jan 1 ──────────────────────────────────────────────
    if now.month == 1 and now.day == 1:
        print("\nYearly aggregations (Jan 1 run)...")
        aggregate_traffic_yearly(prev.year)
        aggregate_citibike_yearly(prev.year)
        aggregate_bus_yearly(prev.year)


if __name__ == '__main__':
    run_aggregations()