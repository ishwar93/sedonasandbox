# Deterministic query catalog — SQL templates and example questions

This document accompanies the L4 deterministic retrieval layer: each entry is a **fixed SQL template** (parameterized) plus **example natural-language questions** users might ask.

**Conventions**

- Placeholders use `:param` style; the app binds values and enforces limits (never raw user SQL).
- Tables reflect the NYC Transit stack: GTFS, `apitable_combined_locations`, Citi Bike, service alerts, route geometry, aggregates, and optional `geo_boundaries` / `geo_aliases` for borough / neighborhood / ZIP.
- Spatial functions (`ST_Point`, `ST_Contains`, `ST_Distance`) assume Sedona/Spark-compatible geometry in `geo_boundaries.geom` and WGS84 lon/lat for points.

### User language vs internal parameters (landmarks, walking distance, H3)

Users rarely speak in **decimal lat/lon** or **H3 indices**. They say things like: *“Citi Bike stations within walking distance of MSG”* or *“subway stops near Penn Station.”*

**How the pipeline should treat that (high level):**

1. **Resolve place language first** (deterministic catalog + optional geocoder): landmark / venue / “MSG” → one anchor **point** `(lat, lon)` and/or metadata (`place_id`, confidence). If ambiguous → `needs_disambiguation` on slot `anchor_place`, not inside SQL.
2. **Resolve “walking distance”** into a **geometry the database understands**: e.g. a **buffer** (meters), **network isochrone**, or **precomputed walk shed**—still not user-facing. Only then do templates see `:lat`, `:lon`, `:radius_m` or a polygon WKT / boundary id.
3. **SQL templates stay point/polygon/H3-shaped** on purpose: they are the **execution layer**, not the **question layer**. Example NL in this doc that mention lat/lon are shorthand for *“after geocoding / anchor resolution.”*
4. **H3** (`h3_r8`, `h3_r9`): treat as an **internal vehicle** for the app—hex aggregation, heatmaps, comparing density / nearness at scale, or bridging “near here” without huge radius scans. Users should not need to name H3 cells; the client or server fills `:h3_id` / resolution after map focus, isochrone-to-hex, or “ring around anchor” logic.

**Doc gap to close over time:** add explicit templates such as `*_within_meters_of_anchor` or `*_within_polygon` once anchor resolution + buffer/isochrone generation exist; keep landmark strings out of raw SQL.

---

## 1. Stops and combined locations

### `stops_list_by_mode`

**Example NL:** “List all subway stops.” / “Show me every bus stop in the dataset.”

```sql
SELECT location_id AS stop_id, location_name AS stop_name, lat, lon, location_type AS feed_type
FROM transit.apitable_combined_locations
WHERE location_type = :mode
  AND lat IS NOT NULL AND lon IS NOT NULL
ORDER BY location_name
LIMIT :limit OFFSET :offset
```

### `stops_count_by_mode`

**Example NL:** “How many subway stops are there?” / “What’s the total number of bus stops?”

```sql
SELECT COUNT(DISTINCT location_id) AS stop_count
FROM transit.apitable_combined_locations
WHERE location_type = :mode
```

### `locations_count_all`

**Example NL:** “Break down how many locations we have by type—subway, bus, Citi Bike.”

```sql
SELECT location_type, COUNT(*) AS n
FROM transit.apitable_combined_locations
GROUP BY location_type
ORDER BY location_type
```

### `stops_by_name_prefix`

**Example NL:** “Find stops whose names start with Times.” / “Show stations matching Astoria.”

```sql
SELECT location_id AS stop_id, location_name AS stop_name, lat, lon, location_type
FROM transit.apitable_combined_locations
WHERE LOWER(location_name) LIKE CONCAT(LOWER(:name_prefix), '%')
ORDER BY location_name
LIMIT :limit
```

### `stops_near_point`

**Example NL (user-shaped):** “What are the 10 closest subway stops to Madison Square Garden?” / “Nearest bus stops to Penn Station.” *(Anchor resolved to coordinates before this query runs.)*

**Example NL (dev / map-pick):** “Nearest stops to this lat/lon.” / “Nearest transit stops to 40.75, -73.99.”

```sql
SELECT location_id AS stop_id, location_name AS stop_name, location_type, lat, lon,
       ST_Distance(
         ST_Point(CAST(lon AS DOUBLE), CAST(lat AS DOUBLE)),
         ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
       ) AS distance
FROM transit.apitable_combined_locations
WHERE lat IS NOT NULL AND lon IS NOT NULL
  AND (:mode IS NULL OR location_type = :mode)
ORDER BY distance
LIMIT :k
```

### `stops_by_h3_cell`

**Example NL (internal / map visualization):** “Stops in the hex cell under the map cursor” / aggregation for walkability view. *(Not something end users phrase as “resolution 9”; the app supplies `:h3_id` and `:resolution`.)*

**Example NL (catalog testing only):** “List all stops in this H3 cell at resolution 9.”

```sql
SELECT location_id AS stop_id, location_name AS stop_name, location_type, lat, lon
FROM transit.apitable_combined_locations
WHERE (:resolution = 8 AND h3_r8 = :h3_id)
   OR (:resolution = 9 AND h3_r9 = :h3_id)
ORDER BY location_name
LIMIT :limit
```

### `stops_list_by_mode_gtfs_fallback` (same intent, alternate table path)

**Example NL:** “List subway stops from GTFS when the combined table isn’t available.”

```sql
SELECT DISTINCT
  gs.stop_id,
  gs.stop_name,
  gs.lat,
  gs.lon,
  :mode AS feed_type
FROM transit.gtfs_stops gs
JOIN transit.gtfs_feed_versions gfv ON gs.feed_id = gfv.feed_id
WHERE gfv.feed_type = :mode
  AND (:location_type_filter_sql)
  AND gs.lat IS NOT NULL AND gs.lon IS NOT NULL
ORDER BY gs.stop_name
LIMIT :limit
```

*Note:* `:location_type_filter_sql` is app-composed for subway vs bus `location_type` rules (mirror `api/routers/stops.py`), not user text.

---

## 2. Citi Bike — stations and live status

### `citibike_station_list`

**Example NL:** “List all Citi Bike stations with capacity.” / “Show every bike share station.”

```sql
SELECT c.location_id AS station_id, c.location_name AS station_name, c.lat, c.lon, s.capacity
FROM transit.apitable_combined_locations c
LEFT JOIN transit.citibike_stations s ON c.location_id = s.station_id
WHERE c.location_type = 'citibike'
  AND c.lat IS NOT NULL AND c.lon IS NOT NULL
ORDER BY c.location_name
LIMIT :limit OFFSET :offset
```

### `citibike_station_list_direct`

**Example NL:** “Get Citi Bike stations straight from the stations table.”

```sql
SELECT station_id, station_name, lat, lon, capacity
FROM transit.citibike_stations
WHERE lat IS NOT NULL AND lon IS NOT NULL
ORDER BY station_name
LIMIT :limit OFFSET :offset
```

### `citibike_station_count`

**Example NL:** “How many Citi Bike stations do we have?”

```sql
SELECT COUNT(*) AS station_count
FROM transit.citibike_stations
```

### `citibike_station_latest_status`

**Example NL:** “What’s the current bike and dock availability for station 123?” / “Latest status for Citi Bike station ABC.”

```sql
WITH latest AS (
  SELECT station_id, MAX(ingested_at) AS max_ts
  FROM transit.citibike_status
  GROUP BY station_id
)
SELECT s.station_id, cs.station_name,
       s.bikes_available, s.ebikes_available, s.docks_available,
       s.is_renting, s.is_returning, s.ingested_at
FROM transit.citibike_status s
JOIN latest l ON s.station_id = l.station_id AND s.ingested_at = l.max_ts
LEFT JOIN transit.citibike_stations cs ON s.station_id = cs.station_id
WHERE s.station_id = :station_id
```

### `citibike_station_has_bikes`

**Example NL:** “Are there any bikes available at station 872?” / “Can I rent a bike at this dock?”

```sql
WITH latest AS (
  SELECT station_id, MAX(ingested_at) AS max_ts
  FROM transit.citibike_status
  GROUP BY station_id
)
SELECT s.station_id,
       CASE WHEN s.bikes_available > 0 THEN true ELSE false END AS has_bikes
FROM transit.citibike_status s
JOIN latest l ON s.station_id = l.station_id AND s.ingested_at = l.max_ts
WHERE s.station_id = :station_id
```

### `citibike_station_has_docks`

**Example NL:** “Does this station have open docks to return a bike?” / “How many free docks—actually, just yes or no for station X.”

```sql
WITH latest AS (
  SELECT station_id, MAX(ingested_at) AS max_ts
  FROM transit.citibike_status
  GROUP BY station_id
)
SELECT s.station_id,
       CASE WHEN s.docks_available > 0 THEN true ELSE false END AS has_docks
FROM transit.citibike_status s
JOIN latest l ON s.station_id = l.station_id AND s.ingested_at = l.max_ts
WHERE s.station_id = :station_id
```

### `citibike_latest_empty_stations`

**Example NL:** “Which stations currently have zero bikes?” / “Show me docks that are completely empty right now.”

```sql
WITH latest AS (
  SELECT station_id, MAX(ingested_at) AS max_ts
  FROM transit.citibike_status
  GROUP BY station_id
)
SELECT s.station_id, cs.station_name, s.bikes_available, s.docks_available, s.ingested_at
FROM transit.citibike_status s
JOIN latest l ON s.station_id = l.station_id AND s.ingested_at = l.max_ts
LEFT JOIN transit.citibike_stations cs ON s.station_id = cs.station_id
WHERE s.bikes_available = 0
ORDER BY s.docks_available DESC
LIMIT :limit
```

### `citibike_latest_full_stations`

**Example NL:** “Which Citi Bike stations have no empty docks?” / “Stations that are full—can’t return a bike.”

```sql
WITH latest AS (
  SELECT station_id, MAX(ingested_at) AS max_ts
  FROM transit.citibike_status
  GROUP BY station_id
)
SELECT s.station_id, cs.station_name, s.bikes_available, s.docks_available, s.ingested_at
FROM transit.citibike_status s
JOIN latest l ON s.station_id = l.station_id AND s.ingested_at = l.max_ts
LEFT JOIN transit.citibike_stations cs ON s.station_id = cs.station_id
WHERE s.docks_available = 0
ORDER BY s.bikes_available DESC
LIMIT :limit
```

---

## 3. Service alerts

### `alerts_active_list`

**Example NL:** “What service alerts are active right now?” / “Show me subway disruptions that are live.” / “List planned work alerts only.”

```sql
SELECT alert_id, feed_source, header_text_plain, mercury_alert_type,
       is_planned, is_active_now,
       CAST(first_seen_at AS STRING) AS first_seen_at,
       CAST(last_seen_at  AS STRING) AS last_seen_at
FROM transit.service_alerts
WHERE is_active_now = true
  AND (:feed_source IS NULL OR feed_source = :feed_source)
  AND (:is_planned IS NULL OR is_planned = :is_planned)
ORDER BY last_seen_at DESC
LIMIT :limit
```

### `alerts_active_count`

**Example NL:** “How many active alerts are there?” / “Count live bus alerts.”

```sql
SELECT COUNT(*) AS active_alert_count
FROM transit.service_alerts
WHERE is_active_now = true
  AND (:feed_source IS NULL OR feed_source = :feed_source)
```

### `alert_by_id`

**Example NL:** “Show me everything you have for alert lmm:alert:12345.”

```sql
SELECT *
FROM transit.service_alerts
WHERE alert_id = :alert_id
LIMIT 1
```

### `alerts_entities_for_active`

**Example NL:** “Which routes and stops are tied to active alerts?” / “Affected entities for all current disruptions.”

```sql
SELECT sae.alert_id, sae.agency_id, sae.route_id, sae.stop_id, sae.priority_level
FROM transit.service_alert_affected_entities sae
JOIN transit.service_alerts sa ON sae.alert_id = sa.alert_id
WHERE sa.is_active_now = true
  AND (:route_id IS NULL OR sae.route_id = :route_id)
  AND (:stop_id IS NULL OR sae.stop_id = :stop_id)
  AND (:agency_id IS NULL OR sae.agency_id = :agency_id)
ORDER BY sae.priority_level DESC
LIMIT :limit
```

### `alerts_affecting_route`

**Example NL:** “What active alerts mention the Q train route?” / “Disruptions affecting route_id XYZ.”

```sql
SELECT sa.alert_id, sa.feed_source, sa.header_text_plain, sa.mercury_alert_type, sa.last_seen_at
FROM transit.service_alerts sa
JOIN transit.service_alert_affected_entities sae ON sa.alert_id = sae.alert_id
WHERE sa.is_active_now = true
  AND sae.route_id = :route_id
ORDER BY sa.last_seen_at DESC
LIMIT :limit
```

### `alerts_affecting_stop`

**Example NL:** “Any active alerts for stop A12?” / “Service changes affecting this station ID.”

```sql
SELECT sa.alert_id, sa.feed_source, sa.header_text_plain, sa.mercury_alert_type, sa.last_seen_at
FROM transit.service_alerts sa
JOIN transit.service_alert_affected_entities sae ON sa.alert_id = sae.alert_id
WHERE sa.is_active_now = true
  AND sae.stop_id = :stop_id
ORDER BY sa.last_seen_at DESC
LIMIT :limit
```

### `alert_active_periods_by_alert`

**Example NL:** “When is alert X supposed to be in effect—start and end times?”

```sql
SELECT alert_id, period_seq, starts_at, ends_at, ingested_at
FROM transit.service_alert_active_periods
WHERE alert_id = :alert_id
ORDER BY period_seq
```

---

## 4. Routes, shapes, and stop connectivity

### `route_geometry_by_route_id`

**Example NL:** “Show the map geometry for the B63 route.” / “Line shapes for route_id 1.”

```sql
SELECT feed_id, route_id, shape_id, route_short_name, route_color, point_count, line_geometry
FROM transit.apitable_routegeom
WHERE route_id = :route_id
  AND (:feed_id IS NULL OR feed_id = :feed_id)
LIMIT :limit
```

### `route_geometry_count`

**Example NL:** “How many route shape records are in apitable_routegeom?”

```sql
SELECT COUNT(*) AS route_geom_count
FROM transit.apitable_routegeom
```

### `route_metadata_lookup`

**Example NL:** “What’s the short name and color for this route?” / “GTFS route metadata for route X.”

```sql
SELECT feed_id, route_id, agency_id, route_short_name, route_type, route_color
FROM transit.gtfs_routes
WHERE route_id = :route_id
  AND (:feed_id IS NULL OR feed_id = :feed_id)
```

### `route_stop_connections`

**Example NL:** “Show scheduled stop-to-stop edges for the M15.” / “Connections on this route in one direction.”

```sql
SELECT feed_id, route_id, direction_id, from_stop_id, to_stop_id, scheduled_travel_time_sec
FROM transit.gtfs_stop_connections
WHERE route_id = :route_id
  AND (:feed_id IS NULL OR feed_id = :feed_id)
  AND (:direction_id IS NULL OR direction_id = :direction_id)
ORDER BY scheduled_travel_time_sec ASC
LIMIT :limit
```

### `stop_connectivity_top_outbound`

**Example NL:** “Which stops have the most outgoing connections in the graph?”

```sql
SELECT from_stop_id, COUNT(*) AS outbound_edges
FROM transit.gtfs_stop_connections
GROUP BY from_stop_id
ORDER BY outbound_edges DESC
LIMIT :limit
```

### `trips_for_route`

**Example NL:** “How many trips use this route?” / “List trip_ids for route Z.”

```sql
SELECT trip_id, route_id, service_id, direction_id, shape_id
FROM transit.gtfs_trips
WHERE route_id = :route_id
  AND (:feed_id IS NULL OR feed_id = :feed_id)
LIMIT :limit
```

### `transfers_from_stop`

**Example NL:** “What transfers exist from stop S?”

```sql
SELECT from_stop_id, to_stop_id, transfer_type, min_transfer_time, feed_id
FROM transit.gtfs_transfers
WHERE from_stop_id = :stop_id
  AND (:feed_id IS NULL OR feed_id = :feed_id)
LIMIT :limit
```

### `calendar_for_service`

**Example NL:** “Which days of the week does service_id WK1 run?”

```sql
SELECT feed_id, service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday
FROM transit.gtfs_calendar
WHERE service_id = :service_id
  AND (:feed_id IS NULL OR feed_id = :feed_id)
LIMIT 1
```

---

## 5. Borough, neighborhood, ZIP — boundaries (requires `geo_boundaries` / `geo_aliases`)

*Neighborhood polygons should come from an authoritative source (e.g. NYC NTA 2020). ZIP from ZCTA/ZIP polygons. Borough from NYC borough boundaries.*

### `boundary_lookup_exact`

**Example NL:** “Resolve the boundary named Astoria as a neighborhood.”

```sql
SELECT boundary_type, boundary_id, boundary_name, borough_name
FROM transit.geo_boundaries
WHERE boundary_type = :boundary_type
  AND LOWER(boundary_name) = LOWER(:boundary_name)
LIMIT 5
```

### `boundary_lookup_alias`

**Example NL:** “User said FiDi—what canonical boundary does that map to?”

```sql
SELECT boundary_type, boundary_id, boundary_name, priority
FROM transit.geo_aliases
WHERE LOWER(alias_text) = LOWER(:alias_text)
ORDER BY priority DESC
LIMIT 10
```

### `boundary_name_candidates_like`

**Example NL:** “User typed Midtown—show possible matching boundaries.”

```sql
SELECT boundary_type, boundary_id, boundary_name
FROM transit.geo_boundaries
WHERE boundary_type IN ('borough', 'neighborhood', 'zip')
  AND LOWER(boundary_name) LIKE CONCAT('%', LOWER(:user_phrase), '%')
ORDER BY boundary_type, boundary_name
LIMIT 10
```

### `locations_in_boundary`

**Example NL:** “List all subway stops in Queens.” / “Citi Bike stations in ZIP 10019.” / “Everything in neighborhood NTA X.”

```sql
SELECT l.location_id, l.location_name, l.location_type, l.lat, l.lon
FROM transit.apitable_combined_locations l
JOIN transit.geo_boundaries b
  ON b.boundary_type = :boundary_type
 AND b.boundary_id = :boundary_id
WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL
  AND ST_Contains(
        b.geom,
        ST_Point(CAST(l.lon AS DOUBLE), CAST(l.lat AS DOUBLE))
      )
  AND (:location_type IS NULL OR l.location_type = :location_type)
ORDER BY l.location_name
LIMIT :limit
```

### `locations_count_in_boundary`

**Example NL:** “How many bus stops are in Brooklyn?” / “Count transit points in this ZIP.”

```sql
SELECT l.location_type, COUNT(*) AS n
FROM transit.apitable_combined_locations l
JOIN transit.geo_boundaries b
  ON b.boundary_type = :boundary_type
 AND b.boundary_id = :boundary_id
WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL
  AND ST_Contains(
        b.geom,
        ST_Point(CAST(l.lon AS DOUBLE), CAST(l.lat AS DOUBLE))
      )
GROUP BY l.location_type
ORDER BY l.location_type
```

### `citibike_in_boundary_with_latest_status`

**Example NL:** “Citi Bike stations in Manhattan with current bikes and docks.” / “Availability in Astoria polygon.”

```sql
WITH latest AS (
  SELECT station_id, MAX(ingested_at) AS max_ts
  FROM transit.citibike_status
  GROUP BY station_id
),
cur AS (
  SELECT s.station_id, s.bikes_available, s.ebikes_available, s.docks_available, s.ingested_at
  FROM transit.citibike_status s
  JOIN latest l ON s.station_id = l.station_id AND s.ingested_at = l.max_ts
)
SELECT cs.station_id, cs.station_name, cs.lat, cs.lon, cs.capacity,
       cur.bikes_available, cur.ebikes_available, cur.docks_available, cur.ingested_at
FROM transit.citibike_stations cs
JOIN cur ON cs.station_id = cur.station_id
JOIN transit.geo_boundaries b
  ON b.boundary_type = :boundary_type
 AND b.boundary_id = :boundary_id
WHERE ST_Contains(b.geom, ST_Point(CAST(cs.lon AS DOUBLE), CAST(cs.lat AS DOUBLE)))
ORDER BY cs.station_name
LIMIT :limit
```

### `alerts_in_boundary_via_affected_stops`

**Example NL:** “Which active alerts affect stops inside Williamsburg?” / “Disruptions touching ZIP 11211.”

```sql
SELECT DISTINCT sa.alert_id, sa.feed_source, sa.header_text_plain, sa.mercury_alert_type, sa.last_seen_at
FROM transit.service_alerts sa
JOIN transit.service_alert_affected_entities sae ON sa.alert_id = sae.alert_id
JOIN transit.apitable_combined_locations l ON sae.stop_id = l.location_id
JOIN transit.geo_boundaries b
  ON b.boundary_type = :boundary_type
 AND b.boundary_id = :boundary_id
WHERE sa.is_active_now = true
  AND l.lat IS NOT NULL AND l.lon IS NOT NULL
  AND ST_Contains(b.geom, ST_Point(CAST(l.lon AS DOUBLE), CAST(l.lat AS DOUBLE)))
ORDER BY sa.last_seen_at DESC
LIMIT :limit
```

---

## 6. Cross-domain (alerts + routes + proximity)

### `alerts_with_route_geometry_for_route`

**Example NL:** “Show active alerts for route Q plus its line geometry.”

```sql
SELECT sa.alert_id, sa.header_text_plain, sae.route_id, rg.route_short_name, rg.route_color, rg.line_geometry
FROM transit.service_alerts sa
JOIN transit.service_alert_affected_entities sae ON sa.alert_id = sae.alert_id
LEFT JOIN transit.apitable_routegeom rg
  ON sae.route_id = rg.route_id AND (:feed_id IS NULL OR rg.feed_id = :feed_id)
WHERE sa.is_active_now = true
  AND sae.route_id = :route_id
LIMIT :limit
```

### `disrupted_stops_near_point`

**Example NL:** “Stops with active alert impact nearest to my location.”

```sql
SELECT DISTINCT l.location_id, l.location_name, l.location_type, l.lat, l.lon,
       ST_Distance(
         ST_Point(CAST(l.lon AS DOUBLE), CAST(l.lat AS DOUBLE)),
         ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
       ) AS distance
FROM transit.service_alerts sa
JOIN transit.service_alert_affected_entities sae ON sa.alert_id = sae.alert_id
JOIN transit.apitable_combined_locations l ON sae.stop_id = l.location_id
WHERE sa.is_active_now = true
  AND l.lat IS NOT NULL AND l.lon IS NOT NULL
ORDER BY distance
LIMIT :limit
```

---

## 7. Aggregates and trends (optional catalog tier)

### `citibike_daily_reliability_by_station`

**Example NL:** “Reliability score for station 123 over the last two weeks.”

```sql
SELECT station_id, calendar_date, reliability_score, pct_time_empty, pct_time_full, sample_count
FROM transit.agg_citibike_daily
WHERE station_id = :station_id
  AND calendar_date BETWEEN :start_date AND :end_date
ORDER BY calendar_date
```

### `citibike_top_unreliable_stations`

**Example NL:** “Which stations had the worst bike availability last month?”

```sql
SELECT station_id, AVG(reliability_score) AS avg_reliability
FROM transit.agg_citibike_daily
WHERE calendar_date BETWEEN :start_date AND :end_date
GROUP BY station_id
ORDER BY avg_reliability ASC
LIMIT :limit
```

### `traffic_speed_by_borough_daily`

**Example NL:** “Average traffic speed in Manhattan by day last week.”

```sql
SELECT borough, calendar_date, avg_speed, sample_count
FROM transit.agg_traffic_daily
WHERE borough = :borough
  AND calendar_date BETWEEN :start_date AND :end_date
ORDER BY calendar_date
```

### `bus_load_daily_by_line`

**Example NL:** “Bus crowding trend for the B44 over January.”

```sql
SELECT line_name, calendar_date, avg_passenger_load, pct_full, sample_count
FROM transit.agg_bus_daily
WHERE line_name = :line_name
  AND calendar_date BETWEEN :start_date AND :end_date
ORDER BY calendar_date
```

---

## 8. Real-time / snapshot feeds (where exposed)

*These tables exist in ingestion; only add to the public catalog if you intend to expose them with the same safety model.*

### `subway_positions_latest`

**Example NL:** “Where are subway trains right now for route 1?”

```sql
SELECT trip_id, route_id, direction, location_stop, location_status, next_arrival, last_update, ingested_at
FROM transit.subway_positions
WHERE (:route_id IS NULL OR route_id = :route_id)
ORDER BY ingested_at DESC
LIMIT :limit
```

### `bus_positions_recent`

**Example NL:** “Recent bus positions on line B41.”

```sql
SELECT line_name, vehicle_ref, lat, lon, passenger_count, passenger_capacity, ingested_at
FROM transit.bus_positions
WHERE line_name = :line_name
  AND ingested_at >= :since_ts
ORDER BY ingested_at DESC
LIMIT :limit
```

### `traffic_speeds_recent_by_borough`

**Example NL:** “Latest traffic speed samples in Brooklyn.”

```sql
SELECT segment_id, borough, speed, travel_time, ingested_at
FROM transit.traffic_speeds
WHERE borough = :borough
  AND ingested_at >= :since_ts
ORDER BY ingested_at DESC
LIMIT :limit
```

---

## 9. Feed provenance and ops

### `gtfs_feed_versions_list`

**Example NL:** “Which GTFS feeds are loaded and what are their etags?”

```sql
SELECT feed_id, feed_type, source_url, etag, downloaded_at
FROM transit.gtfs_feed_versions
ORDER BY downloaded_at DESC
```

### `gtfs_feed_version_for_feed`

**Example NL:** “What version of the subway GTFS is stored for feed xyz?”

```sql
SELECT feed_id, feed_type, etag, downloaded_at
FROM transit.gtfs_feed_versions
WHERE feed_id = :feed_id
LIMIT 1
```

---

## 10. OSM NYC businesses (`ingest_osm.py`)

Data comes from the Overpass API inside the NYC bbox. Only elements with a **name**, coordinates, and an `osm_value` present in `OSM_CATEGORY_MAP` are loaded. Schema in code:

| Table | Grain | Key columns |
|-------|--------|-------------|
| `transit.osm_business` | one row per OSM element | `osm_id` (`node_…` / `way_…` / `relation_…`), `osm_type`, `name`, `osm_key`, `osm_value`, `lat`, `lon`, `address`, `postal_code`, `ingested_at` |
| `transit.osm_business_categories` | one row per (business, category label) | `osm_id`, `category` — primary category from map + optional **cuisine** rows for food amenities |
| `transit.osm_business_hours` | one row per (business, day, open/close) | `osm_id`, `day_of_week` (e.g. `Monday`), `open_time`, `close_time` — from simplified `opening_hours` parse; complex OSM rules omitted |

**User language:** same as elsewhere — *“coffee near MSG”* resolves to anchor + radius/boundary before SQL sees `:lat`/`:lon` or polygon. **Bike rack vs Citi Bike:** OSM `amenity=bicycle_parking` etc. are **not** in the current category map if not listed in `OSM_CATEGORY_MAP`; those rows are skipped at ingest. Disambiguation stays in the NL layer.

---

### `osm_business_count_total`

**Example NL:** “How many NYC businesses did we load from OSM?” / “Size of the POI table.”

```sql
SELECT COUNT(*) AS business_count
FROM transit.osm_business
```

### `osm_business_by_osm_id`

**Example NL:** “Show the OSM record for node 12345.” / “Details for way_987654.”

```sql
SELECT osm_id, osm_type, name, osm_key, osm_value, lat, lon, address, postal_code, ingested_at
FROM transit.osm_business
WHERE osm_id = :osm_id
LIMIT 1
```

### `osm_business_search_name`

**Example NL:** “Find businesses whose name contains Joe’s.” / “Search OSM names for Pizza.”

```sql
SELECT osm_id, osm_type, name, osm_key, osm_value, lat, lon, address, postal_code
FROM transit.osm_business
WHERE LOWER(name) LIKE CONCAT('%', LOWER(:name_substring), '%')
ORDER BY name
LIMIT :limit
```

### `osm_business_list_by_osm_value`

**Example NL:** “List all cafes from OSM.” / “Show every pharmacy in the dataset.”

```sql
SELECT osm_id, name, osm_key, osm_value, lat, lon, address, postal_code
FROM transit.osm_business
WHERE osm_value = :osm_value
ORDER BY name
LIMIT :limit OFFSET :offset
```

### `osm_business_list_by_osm_key`

**Example NL:** “Everything tagged under shop.” / “All tourism POIs we ingested.”

```sql
SELECT osm_id, name, osm_value, lat, lon, address, postal_code
FROM transit.osm_business
WHERE osm_key = :osm_key
ORDER BY osm_value, name
LIMIT :limit OFFSET :offset
```

### `osm_business_count_by_osm_key_value`

**Example NL:** “How many restaurants vs fast_food rows?”

```sql
SELECT osm_key, osm_value, COUNT(*) AS n
FROM transit.osm_business
GROUP BY osm_key, osm_value
ORDER BY n DESC
LIMIT :limit
```

### `osm_business_list_by_postal_code`

**Example NL:** “Businesses in ZIP 10001.” / “What’s in postcode 11201?”

```sql
SELECT osm_id, name, osm_key, osm_value, lat, lon, address, postal_code
FROM transit.osm_business
WHERE postal_code = :postal_code
ORDER BY name
LIMIT :limit
```

### `osm_business_near_point`

**Example NL:** “Coffee shops within 500 m of this point.” *(Anchor from landmark geocoding → `:lat`, `:lon`, `:radius_m`.)*

```sql
SELECT b.osm_id, b.name, b.osm_key, b.osm_value, b.lat, b.lon, b.address,
       ST_Distance(
         ST_Point(CAST(b.lon AS DOUBLE), CAST(b.lat AS DOUBLE)),
         ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
       ) AS distance_m
FROM transit.osm_business b
WHERE b.lat IS NOT NULL AND b.lon IS NOT NULL
  AND (:osm_value IS NULL OR b.osm_value = :osm_value)
  AND ST_Distance(
        ST_Point(CAST(b.lon AS DOUBLE), CAST(b.lat AS DOUBLE)),
        ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
      ) <= :radius_m
ORDER BY distance_m
LIMIT :limit
```

*Note:* confirm whether `ST_Distance` returns meters or degrees in your Sedona/CRS setup; wrap with `ST_DistanceSphere` or project if needed.

### `osm_business_in_boundary`

**Example NL:** “Restaurants inside Brooklyn boundary.” / “Museums in this NTA polygon.”

```sql
SELECT b.osm_id, b.name, b.osm_key, b.osm_value, b.lat, b.lon
FROM transit.osm_business b
JOIN transit.geo_boundaries gb
  ON gb.boundary_type = :boundary_type
 AND gb.boundary_id = :boundary_id
WHERE b.lat IS NOT NULL AND b.lon IS NOT NULL
  AND (:osm_value IS NULL OR b.osm_value = :osm_value)
  AND ST_Contains(
        gb.geom,
        ST_Point(CAST(b.lon AS DOUBLE), CAST(b.lat AS DOUBLE))
      )
ORDER BY b.name
LIMIT :limit
```

---

### Categories (`osm_business_categories`)

### `osm_business_list_by_display_category`

**Example NL:** “Show all Coffee & Tea places.” / “List Museums category businesses.”

```sql
SELECT DISTINCT b.osm_id, b.name, b.osm_key, b.osm_value, b.lat, b.lon, c.category
FROM transit.osm_business b
JOIN transit.osm_business_categories c ON b.osm_id = c.osm_id
WHERE c.category = :category
ORDER BY b.name
LIMIT :limit OFFSET :offset
```

### `osm_category_counts`

**Example NL:** “Top business categories by count.”

```sql
SELECT category, COUNT(*) AS n
FROM transit.osm_business_categories
GROUP BY category
ORDER BY n DESC
LIMIT :limit
```

### `osm_distinct_businesses_per_category`

**Example NL:** “How many unique venues per category label?”

```sql
SELECT category, COUNT(DISTINCT osm_id) AS business_count
FROM transit.osm_business_categories
GROUP BY category
ORDER BY business_count DESC
LIMIT :limit
```

### `osm_businesses_with_multiple_category_rows`

**Example NL:** “Places that have more than one category row (e.g. cuisine tags).”

```sql
SELECT c.osm_id, b.name, COUNT(*) AS category_row_count
FROM transit.osm_business_categories c
JOIN transit.osm_business b ON c.osm_id = b.osm_id
GROUP BY c.osm_id, b.name
HAVING COUNT(*) > 1
ORDER BY category_row_count DESC
LIMIT :limit
```

### `osm_categories_for_single_business`

**Example NL:** “What categories are attached to this OSM id?”

```sql
SELECT osm_id, category
FROM transit.osm_business_categories
WHERE osm_id = :osm_id
ORDER BY category
```

### `osm_business_with_primary_and_cuisine`

**Example NL:** “Restaurants with their mapped display category and any cuisine sub-tags.”

```sql
SELECT b.osm_id, b.name, b.osm_value,
       COLLECT_SET(c.category) AS categories
FROM transit.osm_business b
JOIN transit.osm_business_categories c ON b.osm_id = c.osm_id
WHERE b.osm_value IN ('restaurant', 'fast_food', 'cafe', 'pub', 'bar', 'food_court')
GROUP BY b.osm_id, b.name, b.osm_value
ORDER BY b.name
LIMIT :limit
```

---

### Hours (`osm_business_hours`)

### `osm_hours_for_business`

**Example NL:** “What hours does this place have on file?” / “Opening times for node_….”

```sql
SELECT osm_id, day_of_week, open_time, close_time
FROM transit.osm_business_hours
WHERE osm_id = :osm_id
ORDER BY
  CASE day_of_week
    WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
    WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6 WHEN 'Sunday' THEN 7
    ELSE 8 END,
  open_time
```

### `osm_businesses_with_hours_on_day`

**Example NL:** “Who has Saturday hours recorded?”

```sql
SELECT DISTINCT b.osm_id, b.name, h.open_time, h.close_time
FROM transit.osm_business_hours h
JOIN transit.osm_business b ON h.osm_id = b.osm_id
WHERE h.day_of_week = :day_of_week
ORDER BY b.name
LIMIT :limit
```

### `osm_businesses_missing_hours`

**Example NL:** “Businesses with no parsed opening_hours rows.”

```sql
SELECT b.osm_id, b.name, b.osm_value, b.lat, b.lon
FROM transit.osm_business b
LEFT JOIN transit.osm_business_hours h ON b.osm_id = h.osm_id
WHERE h.osm_id IS NULL
ORDER BY b.name
LIMIT :limit
```

### `osm_business_full_detail`

**Example NL:** “Everything: core row, categories, and hours for one POI.”

```sql
SELECT b.osm_id, b.osm_type, b.name, b.osm_key, b.osm_value, b.lat, b.lon, b.address, b.postal_code,
       c.category AS category_label,
       h.day_of_week, h.open_time, h.close_time
FROM transit.osm_business b
LEFT JOIN transit.osm_business_categories c ON b.osm_id = c.osm_id
LEFT JOIN transit.osm_business_hours h ON b.osm_id = h.osm_id
WHERE b.osm_id = :osm_id
ORDER BY c.category, h.day_of_week, h.open_time
```

### `osm_open_now_approximate`

**Example NL:** “What’s probably open right now on Monday at 2pm?” *(Fragile: times are strings; 24/7 uses `close_time` = `24:00`. Validate in your SQL dialect.)*

```sql
SELECT DISTINCT b.osm_id, b.name, b.lat, b.lon, h.open_time, h.close_time
FROM transit.osm_business b
JOIN transit.osm_business_hours h ON b.osm_id = h.osm_id
WHERE h.day_of_week = :day_of_week
  AND h.open_time <= :current_hhmm
  AND (h.close_time >= :current_hhmm OR h.close_time = '24:00')
LIMIT :limit
```

---

### Cross-domain (OSM + transit)

### `osm_and_citibike_near_point`

**Example NL:** “Citi Bike docks and cafes within 400 m of Penn Station.” *(Two queries or UNION after anchor resolution.)*

```sql
SELECT 'citibike' AS source, c.location_id AS id, c.location_name AS name, c.lat, c.lon
FROM transit.apitable_combined_locations c
WHERE c.location_type = 'citibike'
  AND ST_Distance(
        ST_Point(CAST(c.lon AS DOUBLE), CAST(c.lat AS DOUBLE)),
        ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
      ) <= :radius_m
UNION ALL
SELECT 'osm' AS source, b.osm_id AS id, b.name, b.lat, b.lon
FROM transit.osm_business b
WHERE b.osm_value IN ('cafe', 'restaurant', 'fast_food')
  AND ST_Distance(
        ST_Point(CAST(b.lon AS DOUBLE), CAST(b.lat AS DOUBLE)),
        ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
      ) <= :radius_m
LIMIT :limit
```

### `osm_count_in_same_zip_as_anchor_poi`

**Example NL:** “How many museums share the same ZIP as this library?”

```sql
SELECT COUNT(*) AS n
FROM transit.osm_business x
WHERE x.postal_code = (SELECT postal_code FROM transit.osm_business WHERE osm_id = :anchor_osm_id)
  AND x.osm_value = :target_osm_value
```

---

## Type → template index (for `query_catalog` grouping)

| Type | Purpose | Example query_ids |
|------|---------|-------------------|
| `list` | Ordered rows + limit/offset | `stops_list_by_mode`, `alerts_active_list` |
| `count` | Single number or grouped counts | `stops_count_by_mode`, `alerts_active_count` |
| `lookup` | One entity by id | `alert_by_id`, `citibike_station_latest_status` |
| `availability` | Latest time-series slice | `citibike_station_has_bikes`, `citibike_latest_empty_stations` |
| `spatial` | Point-in-polygon or distance | `locations_in_boundary`, `stops_near_point` |
| `resolution` | Fuzzy geography → candidates | `boundary_lookup_alias`, `boundary_name_candidates_like` |
| `network` | Graph / schedule edges | `route_stop_connections`, `stop_connectivity_top_outbound` |
| `cross` | Multi-table join | `alerts_with_route_geometry_for_route`, `disrupted_stops_near_point` |
| `trend` | Aggregate tables | `citibike_daily_reliability_by_station`, `traffic_speed_by_borough_daily` |
| `ops` | Metadata / feed health | `gtfs_feed_versions_list` |
| `poi` / `osm` | OSM businesses, categories, hours | `osm_business_list_by_display_category`, `osm_hours_for_business`, `osm_business_near_point` |

---

## Implementation notes

1. **Latest Citi Bike status:** always use `MAX(ingested_at)` per `station_id` before reading measures; `citibike_status` is append-only time series.
2. **GTFS stop_id vs combined `location_id`:** `apitable_combined_locations` uses `concat(feed_id, '_', stop_id)` for GTFS-derived rows; joins to `service_alert_affected_entities.stop_id` must follow the same convention or resolve via a mapping table if IDs differ.
3. **Fuzzy neighborhoods:** never run `locations_in_boundary` until `boundary_id` is resolved; use alias + candidate templates, then user picks one.
4. **Dialect:** adjust `ST_*` function names if your cluster uses a different spatial package version; keep one blessed dialect per environment in code, not in user input.
5. **OSM ingest scope:** only `osm_value` keys in `OSM_CATEGORY_MAP` inside `ingest_osm.py` are stored; expanding POI types (e.g. `bicycle_parking`) requires adding that map entry and re-ingesting.
6. **OSM hours:** `open_time` / `close_time` are varchar fragments from a partial parser; “open now” templates are best-effort and should be validated per warehouse SQL functions (or computed in application code).
