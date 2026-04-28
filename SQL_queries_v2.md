# Deterministic query catalog — SQL templates v2
_Last updated: 2026-04-27 | Status: authoritative_

## Scope and constraints

- **Excluded:** PageRank, walk_edges, business_connectivity (Phase 2 WIP)
- **Excluded:** All `agg_*` aggregate tables (still being built — not yet queryable)
- **Included:** geo_boundaries sourced from **NYNTA 2020** (262 NTAs, DCP) and **nybb** (5 boroughs, DCP)
- **CRS note:** Both DCP datasets are EPSG:2263 (NAD83 / NY State Plane Long Island, feet). Geometries must be reprojected to WGS84 (EPSG:4326) before `ST_Contains` with WGS84 lat/lon points, or use `ST_Transform`. Confirm with your Sedona cluster config.
- **location_id convention in `apitable_combined_locations`:**
  - `location_type = 'citibike'` → `location_id = station_id` (from `citibike_stations`)
  - `location_type IN ('subway', 'bus')` → `location_id = concat(feed_id, '_', stop_id)`
- **Alert ↔ stop join:** `service_alert_affected_entities.stop_id` is the raw GTFS `stop_id`. To join to `apitable_combined_locations`, reconstruct: `concat(sae.agency_feed_id, '_', sae.stop_id)`. Requires knowing the feed_id for the agency — see §3 implementation note.
- **Bus positions:** ingested ~once per hour. Templates avoid sub-hourly precision.
- **OSM category values:** only OSM values present in `OSM_CATEGORY_MAP` are stored. Full list in §10 header.

---

## Template type index

| Type | Purpose |
|------|---------|
| `list` | Ordered rows + limit/offset |
| `count` | Single number or grouped counts |
| `lookup` | Single entity by id |
| `availability` | Latest time-series slice |
| `spatial` | Distance or point-in-polygon |
| `resolution` | Fuzzy string → candidate boundaries or IDs |
| `network` | Schedule graph edges |
| `cross` | Multi-table joins across domains |
| `ops` | Feed health / metadata |
| `poi` | OSM businesses, categories, hours |

---

## 1. Stops and combined locations

### `stops_list_by_mode`
**Type:** list
**Example NL:** "List all subway stops." / "Show every bus stop in the dataset."

```sql
SELECT location_id, location_name AS stop_name, lat, lon, location_type
FROM transit.apitable_combined_locations
WHERE location_type = :mode          -- 'subway' | 'bus' | 'citibike'
  AND lat IS NOT NULL AND lon IS NOT NULL
ORDER BY location_name
LIMIT :limit OFFSET :offset
```

---

### `stops_count_by_mode`
**Type:** count
**Example NL:** "How many subway stops are there?" / "Total number of bus stops?"

```sql
SELECT COUNT(DISTINCT location_id) AS stop_count
FROM transit.apitable_combined_locations
WHERE location_type = :mode
```

---

### `locations_count_all`
**Type:** count
**Example NL:** "Break down how many locations we have by type."

```sql
SELECT location_type, COUNT(*) AS n
FROM transit.apitable_combined_locations
GROUP BY location_type
ORDER BY location_type
```

---

### `stops_by_name_search`
**Type:** resolution
**Example NL:** "Find stops whose names contain Astoria." / "Stations matching Times Sq."

```sql
SELECT location_id, location_name AS stop_name, lat, lon, location_type
FROM transit.apitable_combined_locations
WHERE LOWER(location_name) LIKE CONCAT('%', LOWER(:name_fragment), '%')
  AND (:mode IS NULL OR location_type = :mode)
ORDER BY location_name
LIMIT :limit
```
_Note: returns candidates for disambiguation, not a definitive single result._

---

### `stops_near_point`
**Type:** spatial
**Example NL:** "Nearest subway stops to Penn Station." / "10 closest transit stops to this location." _(Anchor resolved to coordinates before this runs.)_

```sql
SELECT location_id, location_name AS stop_name, location_type, lat, lon,
       ST_Distance(
         ST_Point(CAST(lon AS DOUBLE), CAST(lat AS DOUBLE)),
         ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
       ) AS distance_m
FROM transit.apitable_combined_locations
WHERE lat IS NOT NULL AND lon IS NOT NULL
  AND (:mode IS NULL OR location_type = :mode)
ORDER BY distance_m
LIMIT :k
```

---

### `stops_by_h3_cell`
**Type:** spatial (internal)
**Example NL:** _(App-supplied; not user-facing. Used for map hex-cell drill-down.)_

```sql
SELECT location_id, location_name AS stop_name, location_type, lat, lon
FROM transit.apitable_combined_locations
WHERE (:resolution = 8 AND h3_r8 = :h3_id)
   OR (:resolution = 9 AND h3_r9 = :h3_id)
ORDER BY location_name
LIMIT :limit
```

---

## 2. Citi Bike — stations and live status

### `citibike_station_list`
**Type:** list
**Example NL:** "List all Citi Bike stations with capacity."

```sql
SELECT cs.station_id, cs.station_name, cs.lat, cs.lon, cs.capacity
FROM transit.citibike_stations cs
WHERE cs.lat IS NOT NULL AND cs.lon IS NOT NULL
ORDER BY cs.station_name
LIMIT :limit OFFSET :offset
```

---

### `citibike_station_count`
**Type:** count
**Example NL:** "How many Citi Bike stations do we have?"

```sql
SELECT COUNT(*) AS station_count
FROM transit.citibike_stations
```

---

### `citibike_station_latest_status`
**Type:** lookup
**Example NL:** "Current bike and dock availability for station 123." / "Latest status for Citi Bike station ABC."

```sql
WITH latest AS (
  SELECT station_id, MAX(ingested_at) AS max_ts
  FROM transit.citibike_status
  WHERE station_id = :station_id
  GROUP BY station_id
)
SELECT s.station_id, cs.station_name,
       s.bikes_available, s.ebikes_available, s.docks_available,
       s.is_renting, s.is_returning, s.ingested_at
FROM transit.citibike_status s
JOIN latest l ON s.station_id = l.station_id AND s.ingested_at = l.max_ts
LEFT JOIN transit.citibike_stations cs ON s.station_id = cs.station_id
```

---

### `citibike_station_has_bikes`
**Type:** availability
**Example NL:** "Are there bikes available at station 872?" / "Can I rent here?"

```sql
WITH latest AS (
  SELECT station_id, MAX(ingested_at) AS max_ts
  FROM transit.citibike_status
  WHERE station_id = :station_id
  GROUP BY station_id
)
SELECT s.station_id,
       s.bikes_available,
       s.ebikes_available,
       CASE WHEN s.bikes_available > 0 THEN true ELSE false END AS has_bikes
FROM transit.citibike_status s
JOIN latest l ON s.station_id = l.station_id AND s.ingested_at = l.max_ts
```

---

### `citibike_station_has_docks`
**Type:** availability
**Example NL:** "Does this station have open docks to return a bike?"

```sql
WITH latest AS (
  SELECT station_id, MAX(ingested_at) AS max_ts
  FROM transit.citibike_status
  WHERE station_id = :station_id
  GROUP BY station_id
)
SELECT s.station_id,
       s.docks_available,
       CASE WHEN s.docks_available > 0 THEN true ELSE false END AS has_docks
FROM transit.citibike_status s
JOIN latest l ON s.station_id = l.station_id AND s.ingested_at = l.max_ts
```

---

### `citibike_empty_stations_now`
**Type:** availability
**Example NL:** "Which Citi Bike stations currently have zero bikes?" / "Empty docks right now."

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
ORDER BY cs.station_name
LIMIT :limit
```

---

### `citibike_full_stations_now`
**Type:** availability
**Example NL:** "Which stations have no empty docks?" / "Can't return a bike anywhere—where is full?"

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
ORDER BY cs.station_name
LIMIT :limit
```

---

### `citibike_stations_near_point`
**Type:** spatial
**Example NL:** "Citi Bike stations within 500m of Madison Square Garden." / "Nearest bike docks to Penn Station." _(Anchor resolved first.)_

```sql
WITH latest AS (
  SELECT station_id, MAX(ingested_at) AS max_ts
  FROM transit.citibike_status
  GROUP BY station_id
),
cur AS (
  SELECT s.station_id, s.bikes_available, s.ebikes_available, s.docks_available
  FROM transit.citibike_status s
  JOIN latest l ON s.station_id = l.station_id AND s.ingested_at = l.max_ts
)
SELECT cs.station_id, cs.station_name, cs.lat, cs.lon, cs.capacity,
       cur.bikes_available, cur.ebikes_available, cur.docks_available,
       ST_Distance(
         ST_Point(CAST(cs.lon AS DOUBLE), CAST(cs.lat AS DOUBLE)),
         ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
       ) AS distance_m
FROM transit.citibike_stations cs
LEFT JOIN cur ON cs.station_id = cur.station_id
WHERE cs.lat IS NOT NULL AND cs.lon IS NOT NULL
  AND (:min_bikes IS NULL OR cur.bikes_available >= :min_bikes)
ORDER BY distance_m
LIMIT :k
```
_Note: `:min_bikes` optional — pass NULL to return all, or 1 to filter to stations with available bikes._

---

## 3. Service alerts

### `alerts_active_list`
**Type:** list
**Example NL:** "What service alerts are active right now?" / "Live subway disruptions." / "Planned work alerts only."

```sql
SELECT alert_id, feed_source, header_text_plain, mercury_alert_type,
       is_planned, is_active_now,
       CAST(first_seen_at AS STRING) AS first_seen_at,
       CAST(last_seen_at  AS STRING) AS last_seen_at
FROM transit.service_alerts
WHERE is_active_now = true
  AND (:feed_source IS NULL OR feed_source = :feed_source)   -- 'subway' | 'bus'
  AND (:is_planned  IS NULL OR is_planned  = :is_planned)
ORDER BY last_seen_at DESC
LIMIT :limit
```

---

### `alerts_active_count`
**Type:** count
**Example NL:** "How many active alerts are there?" / "Count live bus alerts."

```sql
SELECT COUNT(*) AS active_alert_count
FROM transit.service_alerts
WHERE is_active_now = true
  AND (:feed_source IS NULL OR feed_source = :feed_source)
```

---

### `alert_by_id`
**Type:** lookup
**Example NL:** "Show everything for alert lmm:alert:12345."

```sql
SELECT *
FROM transit.service_alerts
WHERE alert_id = :alert_id
LIMIT 1
```

---

### `alert_active_periods_by_alert`
**Type:** lookup
**Example NL:** "When is alert X supposed to be in effect—start and end times?"

```sql
SELECT alert_id, period_seq, starts_at, ends_at, ingested_at
FROM transit.service_alert_active_periods
WHERE alert_id = :alert_id
ORDER BY period_seq
```

---

### `alerts_affecting_route`
**Type:** list
**Example NL:** "Active alerts for the Q train." / "Disruptions on route M15."

```sql
SELECT sa.alert_id, sa.feed_source, sa.header_text_plain,
       sa.mercury_alert_type, sa.is_planned, sa.last_seen_at
FROM transit.service_alerts sa
JOIN transit.service_alert_affected_entities sae ON sa.alert_id = sae.alert_id
WHERE sa.is_active_now = true
  AND sae.route_id = :route_id
ORDER BY sa.last_seen_at DESC
LIMIT :limit
```

---

### `alerts_affecting_stop`
**Type:** list
**Example NL:** "Active alerts for stop A12." / "Service changes at this station."
_Note: `:stop_id` here is the raw GTFS stop_id as stored in `service_alert_affected_entities`._

```sql
SELECT sa.alert_id, sa.feed_source, sa.header_text_plain,
       sa.mercury_alert_type, sa.is_planned, sa.last_seen_at
FROM transit.service_alerts sa
JOIN transit.service_alert_affected_entities sae ON sa.alert_id = sae.alert_id
WHERE sa.is_active_now = true
  AND sae.stop_id = :stop_id
ORDER BY sa.last_seen_at DESC
LIMIT :limit
```

---

### `alerts_entities_for_active`
**Type:** list
**Example NL:** "Which routes and stops are tied to active alerts?" / "All affected entities for current disruptions."

```sql
SELECT sae.alert_id, sae.agency_id, sae.route_id, sae.stop_id, sae.priority_level
FROM transit.service_alert_affected_entities sae
JOIN transit.service_alerts sa ON sae.alert_id = sa.alert_id
WHERE sa.is_active_now = true
  AND (:route_id  IS NULL OR sae.route_id  = :route_id)
  AND (:stop_id   IS NULL OR sae.stop_id   = :stop_id)
  AND (:agency_id IS NULL OR sae.agency_id = :agency_id)
ORDER BY sae.priority_level DESC
LIMIT :limit
```

---

### `alerts_by_severity`
**Type:** list
**Example NL:** "Show only suspended service alerts." / "Highest priority disruptions right now."

```sql
SELECT sa.alert_id, sa.feed_source, sa.header_text_plain, sa.mercury_alert_type,
       sae.route_id, sae.stop_id, sae.priority_level
FROM transit.service_alerts sa
JOIN transit.service_alert_affected_entities sae ON sa.alert_id = sae.alert_id
WHERE sa.is_active_now = true
  AND sae.priority_level >= :min_priority        -- 1-35; e.g. 30 for suspensions
  AND (:feed_source IS NULL OR sa.feed_source = :feed_source)
ORDER BY sae.priority_level DESC, sa.last_seen_at DESC
LIMIT :limit
```

---

### `highest_severity_alert_for_route`
**Type:** lookup
**Example NL:** "What's the worst active alert on the A train?" / "Max disruption severity for route G."

```sql
SELECT sa.alert_id, sa.header_text_plain, sa.mercury_alert_type,
       MAX(sae.priority_level) AS max_priority_level
FROM transit.service_alerts sa
JOIN transit.service_alert_affected_entities sae ON sa.alert_id = sae.alert_id
WHERE sa.is_active_now = true
  AND sae.route_id = :route_id
GROUP BY sa.alert_id, sa.header_text_plain, sa.mercury_alert_type
ORDER BY max_priority_level DESC
LIMIT 1
```

---

### `alert_severity_summary`
**Type:** count
**Example NL:** "Break down active alerts by type." / "How many suspensions vs delays right now?"

```sql
SELECT sa.mercury_alert_type, COUNT(DISTINCT sa.alert_id) AS alert_count,
       MAX(sae.priority_level) AS max_priority
FROM transit.service_alerts sa
JOIN transit.service_alert_affected_entities sae ON sa.alert_id = sae.alert_id
WHERE sa.is_active_now = true
  AND (:feed_source IS NULL OR sa.feed_source = :feed_source)
GROUP BY sa.mercury_alert_type
ORDER BY max_priority DESC
```

---

## 4. Routes, shapes, and stop connectivity

### `route_metadata_lookup`
**Type:** lookup
**Example NL:** "What's the short name and color for route 1?" / "GTFS metadata for route M15."

```sql
SELECT feed_id, route_id, agency_id, route_short_name, route_type, route_color
FROM transit.gtfs_routes
WHERE route_id = :route_id
  AND (:feed_id IS NULL OR feed_id = :feed_id)
LIMIT 1
```

---

### `route_search_by_short_name`
**Type:** resolution
**Example NL:** "Find the route for the Q train." / "Which route_id is the M15?"

```sql
SELECT feed_id, route_id, agency_id, route_short_name, route_type
FROM transit.gtfs_routes
WHERE LOWER(route_short_name) = LOWER(:route_short_name)
ORDER BY feed_id
LIMIT :limit
```

---

### `route_geometry_by_route_id`
**Type:** lookup
**Example NL:** "Map geometry for the B63 route." / "Line shape for route_id 1."

```sql
SELECT feed_id, route_id, shape_id, route_short_name, route_color, point_count, geometry
FROM transit.apitable_routegeom
WHERE route_id = :route_id
  AND (:feed_id IS NULL OR feed_id = :feed_id)
LIMIT :limit
```

---

### `route_stop_connections`
**Type:** network
**Example NL:** "Show scheduled stop-to-stop edges for the M15." / "Connections on this route in one direction."

```sql
SELECT feed_id, route_id, direction_id, from_stop_id, to_stop_id, scheduled_travel_time_sec
FROM transit.gtfs_stop_connections
WHERE route_id = :route_id
  AND (:feed_id      IS NULL OR feed_id      = :feed_id)
  AND (:direction_id IS NULL OR direction_id = :direction_id)
ORDER BY scheduled_travel_time_sec ASC
LIMIT :limit
```

---

### `routes_serving_stop`
**Type:** network
**Example NL:** "Which routes pass through stop S123?" / "What trains stop at this station?"
_Note: `:stop_id` is raw GTFS stop_id (without feed_id prefix)._

```sql
SELECT DISTINCT gsc.route_id, gr.route_short_name, gr.route_type, gr.feed_id
FROM transit.gtfs_stop_connections gsc
JOIN transit.gtfs_routes gr
  ON gsc.route_id = gr.route_id AND gsc.feed_id = gr.feed_id
WHERE (gsc.from_stop_id = :stop_id OR gsc.to_stop_id = :stop_id)
  AND (:feed_id IS NULL OR gsc.feed_id = :feed_id)
ORDER BY gr.route_short_name
```

---

### `stop_connectivity_top_outbound`
**Type:** network
**Example NL:** "Which stops have the most outgoing connections?"

```sql
SELECT from_stop_id, COUNT(*) AS outbound_edges
FROM transit.gtfs_stop_connections
GROUP BY from_stop_id
ORDER BY outbound_edges DESC
LIMIT :limit
```

---

### `transfers_from_stop`
**Type:** network
**Example NL:** "What transfers exist from stop S?" / "Can I transfer anywhere from this station?"
_Note: `:stop_id` is raw GTFS stop_id._

```sql
SELECT from_stop_id, to_stop_id, transfer_type, min_transfer_time, feed_id
FROM transit.gtfs_transfers
WHERE from_stop_id = :stop_id
  AND (:feed_id IS NULL OR feed_id = :feed_id)
LIMIT :limit
```

---

### `trips_for_route`
**Type:** list
**Example NL:** "How many trips use route Z?" / "List trip_ids for the A train."

```sql
SELECT trip_id, route_id, service_id, direction_id, shape_id
FROM transit.gtfs_trips
WHERE route_id = :route_id
  AND (:feed_id IS NULL OR feed_id = :feed_id)
LIMIT :limit
```

---

### `calendar_for_service`
**Type:** lookup
**Example NL:** "Which days does service_id WK1 run?"

```sql
SELECT feed_id, service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday
FROM transit.gtfs_calendar
WHERE service_id = :service_id
  AND (:feed_id IS NULL OR feed_id = :feed_id)
LIMIT 1
```

---

## 5. Borough and neighborhood boundaries

_Source: **nybb** (5 borough polygons, DCP) and **nynta2020** (262 NTA polygons, DCP). Both are EPSG:2263; reprojection to WGS84 required for ST_Contains with lat/lon points._

_`geo_boundaries` schema assumed:_
```
boundary_type  varchar   -- 'borough' | 'neighborhood'
boundary_id    varchar   -- BoroCode (1-5) for borough; NTA2020 code (e.g. 'MN2501') for neighborhood
boundary_name  varchar   -- BoroName or NTAName
borough_name   varchar   -- parent borough (populated for neighborhoods; null for boroughs)
nta_type       varchar   -- NTAType from nynta2020: '0'=residential,'9'=park,'8'=airport, etc.
geom           geometry  -- WGS84 polygon
```

_`geo_aliases` schema assumed:_
```
alias_text     varchar   -- user-facing string, e.g. 'FiDi', 'BK', 'LES'
boundary_type  varchar
boundary_id    varchar
boundary_name  varchar
priority       int       -- higher = prefer this match
```

---

### `boundary_lookup_exact`
**Type:** resolution
**Example NL:** "Resolve the boundary named Astoria as a neighborhood." / "Find borough Brooklyn."

```sql
SELECT boundary_type, boundary_id, boundary_name, borough_name, nta_type
FROM transit.geo_boundaries
WHERE boundary_type = :boundary_type          -- 'borough' | 'neighborhood'
  AND LOWER(boundary_name) = LOWER(:boundary_name)
  AND (boundary_type = 'borough' OR nta_type = '0')  -- exclude parks/airports unless requested
LIMIT 5
```

---

### `boundary_lookup_alias`
**Type:** resolution
**Example NL:** "User said FiDi—what boundary does that map to?" / "Resolve BK to Brooklyn."

```sql
SELECT ga.boundary_type, ga.boundary_id, ga.boundary_name, ga.priority
FROM transit.geo_aliases ga
WHERE LOWER(ga.alias_text) = LOWER(:alias_text)
ORDER BY ga.priority DESC
LIMIT 10
```

---

### `boundary_name_candidates_like`
**Type:** resolution
**Example NL:** "User typed Midtown—show matching boundaries." / "Neighborhoods with Jackson in the name."

```sql
SELECT boundary_type, boundary_id, boundary_name, borough_name, nta_type
FROM transit.geo_boundaries
WHERE (:boundary_type IS NULL OR boundary_type = :boundary_type)
  AND LOWER(boundary_name) LIKE CONCAT('%', LOWER(:user_phrase), '%')
ORDER BY boundary_type, boundary_name
LIMIT 10
```

---

### `locations_in_boundary`
**Type:** spatial
**Example NL:** "All subway stops in Queens." / "Citi Bike stations in Astoria." / "Bus stops in ZIP 10019."
_Precondition: `:boundary_id` must be a resolved boundary_id (not a raw name string)._

```sql
SELECT l.location_id, l.location_name, l.location_type, l.lat, l.lon
FROM transit.apitable_combined_locations l
JOIN transit.geo_boundaries b
  ON b.boundary_type = :boundary_type
 AND b.boundary_id   = :boundary_id
WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL
  AND (:location_type IS NULL OR l.location_type = :location_type)
  AND ST_Contains(
        b.geom,
        ST_Point(CAST(l.lon AS DOUBLE), CAST(l.lat AS DOUBLE))
      )
ORDER BY l.location_name
LIMIT :limit
```

---

### `locations_count_in_boundary`
**Type:** count
**Example NL:** "How many bus stops are in Brooklyn?" / "Count transit locations in Williamsburg."

```sql
SELECT l.location_type, COUNT(*) AS n
FROM transit.apitable_combined_locations l
JOIN transit.geo_boundaries b
  ON b.boundary_type = :boundary_type
 AND b.boundary_id   = :boundary_id
WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL
  AND ST_Contains(
        b.geom,
        ST_Point(CAST(l.lon AS DOUBLE), CAST(l.lat AS DOUBLE))
      )
GROUP BY l.location_type
ORDER BY l.location_type
```

---

### `citibike_in_boundary_with_status`
**Type:** spatial + availability
**Example NL:** "Citi Bike stations in Manhattan with current availability." / "Bike docks in Astoria—which have bikes now?"

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
 AND b.boundary_id   = :boundary_id
WHERE ST_Contains(b.geom, ST_Point(CAST(cs.lon AS DOUBLE), CAST(cs.lat AS DOUBLE)))
ORDER BY cs.station_name
LIMIT :limit
```

---

### `neighborhoods_in_borough`
**Type:** list
**Example NL:** "List all neighborhoods in Brooklyn." / "What NTAs are in Queens?"

```sql
SELECT boundary_id AS nta_code, boundary_name AS nta_name, nta_type
FROM transit.geo_boundaries
WHERE boundary_type = 'neighborhood'
  AND LOWER(borough_name) = LOWER(:borough_name)
  AND nta_type = '0'          -- residential NTAs only; remove filter to include parks/airports
ORDER BY boundary_name
```

---

## 6. Cross-domain queries

### `alerts_in_boundary_via_stops`
**Type:** cross
**Example NL:** "Active alerts affecting stops inside Williamsburg." / "Disruptions touching Queens stops."
_Implementation note: join uses raw GTFS stop_id from `sae`. This resolves against `gtfs_stops` (not `apitable_combined_locations`) to avoid the feed_id prefix complication._

```sql
SELECT DISTINCT sa.alert_id, sa.feed_source, sa.header_text_plain,
                sa.mercury_alert_type, sa.is_planned, sa.last_seen_at
FROM transit.service_alerts sa
JOIN transit.service_alert_affected_entities sae ON sa.alert_id = sae.alert_id
JOIN transit.gtfs_stops gs
  ON sae.stop_id = gs.stop_id
 AND sae.agency_id IS NOT NULL
JOIN transit.geo_boundaries b
  ON b.boundary_type = :boundary_type
 AND b.boundary_id   = :boundary_id
WHERE sa.is_active_now = true
  AND gs.lat IS NOT NULL AND gs.lon IS NOT NULL
  AND ST_Contains(b.geom, ST_Point(CAST(gs.lon AS DOUBLE), CAST(gs.lat AS DOUBLE)))
ORDER BY sa.last_seen_at DESC
LIMIT :limit
```

---

### `alerts_with_route_geometry`
**Type:** cross
**Example NL:** "Active alerts for the Q train plus its map geometry."

```sql
SELECT sa.alert_id, sa.header_text_plain, sae.route_id,
       rg.route_short_name, rg.route_color, rg.geometry
FROM transit.service_alerts sa
JOIN transit.service_alert_affected_entities sae ON sa.alert_id = sae.alert_id
LEFT JOIN transit.apitable_routegeom rg
  ON sae.route_id = rg.route_id
 AND (:feed_id IS NULL OR rg.feed_id = :feed_id)
WHERE sa.is_active_now = true
  AND sae.route_id = :route_id
LIMIT :limit
```

---

### `disrupted_stops_near_point`
**Type:** cross + spatial
**Example NL:** "Stops with active alerts near my location." / "Disrupted stations closest to Times Square."

```sql
SELECT DISTINCT
       concat(gs.feed_id, '_', gs.stop_id) AS location_id,
       gs.stop_name, gs.lat, gs.lon,
       ST_Distance(
         ST_Point(CAST(gs.lon AS DOUBLE), CAST(gs.lat AS DOUBLE)),
         ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
       ) AS distance_m
FROM transit.service_alerts sa
JOIN transit.service_alert_affected_entities sae ON sa.alert_id = sae.alert_id
JOIN transit.gtfs_stops gs ON sae.stop_id = gs.stop_id
WHERE sa.is_active_now = true
  AND gs.lat IS NOT NULL AND gs.lon IS NOT NULL
ORDER BY distance_m
LIMIT :limit
```

---

### `citibike_near_disrupted_stops`
**Type:** cross + spatial
**Example NL:** "Citi Bike stations near stops affected by this alert—as an alternative to the subway." / "Bike options when route G is suspended."

```sql
WITH disrupted_stop_coords AS (
  SELECT DISTINCT gs.lat, gs.lon
  FROM transit.service_alert_affected_entities sae
  JOIN transit.gtfs_stops gs ON sae.stop_id = gs.stop_id
  JOIN transit.service_alerts sa ON sae.alert_id = sa.alert_id
  WHERE sa.is_active_now = true
    AND (:route_id IS NULL OR sae.route_id = :route_id)
    AND gs.lat IS NOT NULL
),
latest AS (
  SELECT station_id, MAX(ingested_at) AS max_ts
  FROM transit.citibike_status
  GROUP BY station_id
)
SELECT cs.station_id, cs.station_name, cs.lat, cs.lon,
       cst.bikes_available, cst.ebikes_available,
       MIN(ST_Distance(
         ST_Point(CAST(cs.lon AS DOUBLE), CAST(cs.lat AS DOUBLE)),
         ST_Point(CAST(d.lon AS DOUBLE),  CAST(d.lat AS DOUBLE))
       )) AS min_distance_to_disrupted_stop_m
FROM transit.citibike_stations cs
JOIN transit.citibike_status cst
  ON cs.station_id = cst.station_id
JOIN latest l ON cst.station_id = l.station_id AND cst.ingested_at = l.max_ts
CROSS JOIN disrupted_stop_coords d
WHERE cs.lat IS NOT NULL
GROUP BY cs.station_id, cs.station_name, cs.lat, cs.lon,
         cst.bikes_available, cst.ebikes_available
HAVING MIN(ST_Distance(
         ST_Point(CAST(cs.lon AS DOUBLE), CAST(cs.lat AS DOUBLE)),
         ST_Point(CAST(d.lon AS DOUBLE),  CAST(d.lat AS DOUBLE))
       )) <= :radius_m
ORDER BY min_distance_to_disrupted_stop_m
LIMIT :limit
```

---

### `routes_in_boundary`
**Type:** cross + spatial
**Example NL:** "Which subway routes have stops in Brooklyn?" / "Bus routes serving Astoria."

```sql
SELECT DISTINCT gsc.route_id, gr.route_short_name, gr.route_type, gr.feed_id
FROM transit.gtfs_stop_connections gsc
JOIN transit.gtfs_routes gr ON gsc.route_id = gr.route_id AND gsc.feed_id = gr.feed_id
JOIN transit.gtfs_stops gs
  ON (gsc.from_stop_id = gs.stop_id OR gsc.to_stop_id = gs.stop_id)
 AND gsc.feed_id = gs.feed_id
JOIN transit.geo_boundaries b
  ON b.boundary_type = :boundary_type
 AND b.boundary_id   = :boundary_id
WHERE gs.lat IS NOT NULL AND gs.lon IS NOT NULL
  AND (:route_type IS NULL OR gr.route_type = :route_type)  -- 1=subway, 3=bus
  AND ST_Contains(b.geom, ST_Point(CAST(gs.lon AS DOUBLE), CAST(gs.lat AS DOUBLE)))
ORDER BY gr.route_short_name
```

---

## 7. Real-time snapshot feeds

_These are current-state feeds. Subway positions are overwritten each poll (no history). Bus positions are appended ~hourly; avoid templates requiring sub-hourly precision._

### `subway_positions_now`
**Type:** availability
**Example NL:** "Where are trains on the 1 train right now?" / "Current subway positions for route A."

```sql
SELECT trip_id, route_id, direction, location_stop, location_status,
       next_arrival, last_update, ingested_at
FROM transit.subway_positions
WHERE (:route_id IS NULL OR route_id = :route_id)
ORDER BY ingested_at DESC
LIMIT :limit
```

---

### `bus_positions_by_line`
**Type:** availability
**Example NL:** "Where are B41 buses right now?" / "Recent positions for the M15."
_Cadence: ~hourly. Results reflect last ingestion window, not real-time._

```sql
SELECT vehicle_ref, line_name, lat, lon,
       passenger_count, passenger_capacity,
       distance_from_stop, stops_away, ingested_at
FROM transit.bus_positions
WHERE line_name = :line_name
  AND ingested_at >= :since_ts         -- e.g. current_timestamp - interval 2 hours
ORDER BY ingested_at DESC
LIMIT :limit
```

---

### `bus_lines_active_now`
**Type:** list
**Example NL:** "Which bus lines have position data right now?" / "Active bus routes in our feed."

```sql
SELECT DISTINCT line_name, COUNT(DISTINCT vehicle_ref) AS vehicle_count,
                MAX(ingested_at) AS latest_reading
FROM transit.bus_positions
WHERE ingested_at >= :since_ts
GROUP BY line_name
ORDER BY line_name
```

---

### `buses_near_point`
**Type:** spatial + availability
**Example NL:** "Are there buses near Penn Station right now?" / "Closest active buses to this location."
_Cadence note: uses last ingested batch; resolution is ~hourly, not real-time._

```sql
SELECT vehicle_ref, line_name, lat, lon,
       passenger_count, passenger_capacity,
       ST_Distance(
         ST_Point(CAST(lon AS DOUBLE), CAST(lat AS DOUBLE)),
         ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
       ) AS distance_m,
       ingested_at
FROM transit.bus_positions
WHERE lat IS NOT NULL AND lon IS NOT NULL
  AND ingested_at >= :since_ts
  AND (:line_name IS NULL OR line_name = :line_name)
ORDER BY distance_m
LIMIT :k
```

---

### `traffic_speeds_recent_by_borough`
**Type:** availability
**Example NL:** "Latest traffic speeds in Brooklyn." / "Current road conditions in Manhattan."

```sql
SELECT segment_id, link_name, borough, speed, travel_time, ingested_at
FROM transit.traffic_speeds
WHERE borough = :borough
  AND ingested_at >= :since_ts
ORDER BY ingested_at DESC
LIMIT :limit
```

---

### `traffic_speeds_slowest_segments`
**Type:** availability
**Example NL:** "Which road segments are slowest right now?" / "Worst traffic segments in Queens."

```sql
SELECT segment_id, link_name, borough, speed, travel_time, ingested_at
FROM transit.traffic_speeds
WHERE ingested_at >= :since_ts
  AND (:borough IS NULL OR borough = :borough)
ORDER BY speed ASC
LIMIT :limit
```

---

## 8. OSM businesses (POI)

_OSM values stored are limited to those in `OSM_CATEGORY_MAP`. Full list of stored `osm_value` keys:_
`restaurant, fast_food, cafe, bar, pub, food_court, ice_cream, bakery, biergarten,`
`pharmacy, clinic, dentist, doctors, hospital, veterinary, therapist, fitness_centre,`
`swimming_pool, sports_centre, post_office, library, gym, spa, beauty_salon,`
`hairdresser, laundry, fuel, car_wash, cinema, theatre, nightclub, arts_centre,`
`casino, stripclub, museum, gallery, attraction, school, university, college,`
`kindergarten, language_school, music_school, supermarket, convenience, butcher,`
`deli, seafood, alcohol, greengrocer`

_Display category labels are in `osm_business_categories.category` (mapped values like "Coffee & Tea", "Gyms", etc.)_

---

### `osm_business_count_total`
**Type:** count
**Example NL:** "How many NYC businesses did we load from OSM?" / "Size of the POI table."

```sql
SELECT COUNT(*) AS business_count
FROM transit.osm_business
```

---

### `osm_business_by_id`
**Type:** lookup
**Example NL:** "Show the OSM record for node_12345." / "Details for way_987654."

```sql
SELECT osm_id, osm_type, name, osm_key, osm_value, lat, lon, ingested_at
FROM transit.osm_business
WHERE osm_id = :osm_id
LIMIT 1
```

---

### `osm_business_full_detail`
**Type:** lookup
**Example NL:** "Everything about this POI: core record, categories, and hours."

```sql
SELECT b.osm_id, b.osm_type, b.name, b.osm_key, b.osm_value, b.lat, b.lon,
       c.category AS category_label,
       h.day_of_week, h.open_time, h.close_time
FROM transit.osm_business b
LEFT JOIN transit.osm_business_categories c ON b.osm_id = c.osm_id
LEFT JOIN transit.osm_business_hours h ON b.osm_id = h.osm_id
WHERE b.osm_id = :osm_id
ORDER BY c.category, h.day_of_week, h.open_time
```

---

### `osm_business_search_name`
**Type:** resolution
**Example NL:** "Find businesses whose name contains Joe's." / "Search OSM for businesses named Pizza."

```sql
SELECT osm_id, osm_type, name, osm_key, osm_value, lat, lon
FROM transit.osm_business
WHERE LOWER(name) LIKE CONCAT('%', LOWER(:name_substring), '%')
  AND (:osm_value IS NULL OR osm_value = :osm_value)
ORDER BY name
LIMIT :limit
```

---

### `osm_business_list_by_osm_value`
**Type:** list
**Example NL:** "List all cafes from OSM." / "Every pharmacy we have."

```sql
SELECT osm_id, name, osm_key, osm_value, lat, lon
FROM transit.osm_business
WHERE osm_value = :osm_value
ORDER BY name
LIMIT :limit OFFSET :offset
```

---

### `osm_business_list_by_display_category`
**Type:** list
**Example NL:** "Show all Coffee & Tea places." / "List Museums."

```sql
SELECT DISTINCT b.osm_id, b.name, b.osm_key, b.osm_value, b.lat, b.lon, c.category
FROM transit.osm_business b
JOIN transit.osm_business_categories c ON b.osm_id = c.osm_id
WHERE c.category = :category
ORDER BY b.name
LIMIT :limit OFFSET :offset
```

---

### `osm_business_count_by_type`
**Type:** count
**Example NL:** "How many restaurants vs cafes vs bars?" / "POI breakdown by osm_value."

```sql
SELECT osm_key, osm_value, COUNT(*) AS n
FROM transit.osm_business
GROUP BY osm_key, osm_value
ORDER BY n DESC
LIMIT :limit
```

---

### `osm_category_counts`
**Type:** count
**Example NL:** "Top business categories by count." / "How many venues per display category?"

```sql
SELECT category, COUNT(DISTINCT osm_id) AS business_count
FROM transit.osm_business_categories
GROUP BY category
ORDER BY business_count DESC
LIMIT :limit
```

---

### `osm_business_near_point`
**Type:** spatial
**Example NL:** "Coffee shops within 500m of Penn Station." / "Pharmacies near this location." _(Anchor resolved first.)_

```sql
SELECT b.osm_id, b.name, b.osm_key, b.osm_value, b.lat, b.lon,
       ST_Distance(
         ST_Point(CAST(b.lon AS DOUBLE), CAST(b.lat AS DOUBLE)),
         ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
       ) AS distance_m
FROM transit.osm_business b
WHERE b.lat IS NOT NULL AND b.lon IS NOT NULL
  AND (:osm_value IS NULL OR b.osm_value = :osm_value)
  AND (:category IS NULL OR EXISTS (
        SELECT 1 FROM transit.osm_business_categories c
        WHERE c.osm_id = b.osm_id AND c.category = :category
      ))
  AND ST_Distance(
        ST_Point(CAST(b.lon AS DOUBLE), CAST(b.lat AS DOUBLE)),
        ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
      ) <= :radius_m
ORDER BY distance_m
LIMIT :limit
```

---

### `osm_business_in_boundary`
**Type:** spatial
**Example NL:** "Restaurants inside Brooklyn." / "Museums in the Upper West Side NTA."

```sql
SELECT b.osm_id, b.name, b.osm_key, b.osm_value, b.lat, b.lon
FROM transit.osm_business b
JOIN transit.geo_boundaries gb
  ON gb.boundary_type = :boundary_type
 AND gb.boundary_id   = :boundary_id
WHERE b.lat IS NOT NULL AND b.lon IS NOT NULL
  AND (:osm_value IS NULL OR b.osm_value = :osm_value)
  AND ST_Contains(gb.geom, ST_Point(CAST(b.lon AS DOUBLE), CAST(b.lat AS DOUBLE)))
ORDER BY b.name
LIMIT :limit
```

---

### `osm_business_list_by_postal_code`
**Type:** list
**Example NL:** "Businesses in ZIP 10001." / "What's in postcode 11201?"

```sql
SELECT osm_id, name, osm_key, osm_value, lat, lon
FROM transit.osm_business
WHERE postal_code = :postal_code
  AND (:osm_value IS NULL OR osm_value = :osm_value)
ORDER BY name
LIMIT :limit
```

_Note: `postal_code` is parsed from OSM tags and has ~60-70% coverage. Missing values are NULL, not empty string._

---

### `osm_hours_for_business`
**Type:** lookup
**Example NL:** "What hours does this place have?" / "Opening times for node_123."

```sql
SELECT osm_id, day_of_week, open_time, close_time
FROM transit.osm_business_hours
WHERE osm_id = :osm_id
ORDER BY
  CASE day_of_week
    WHEN 'Monday'    THEN 1 WHEN 'Tuesday'  THEN 2 WHEN 'Wednesday' THEN 3
    WHEN 'Thursday'  THEN 4 WHEN 'Friday'   THEN 5 WHEN 'Saturday'  THEN 6
    WHEN 'Sunday'    THEN 7 ELSE 8
  END,
  open_time
```

---

### `osm_businesses_with_hours_on_day`
**Type:** list
**Example NL:** "Who has Saturday hours recorded?" / "Businesses open on Sunday."

```sql
SELECT DISTINCT b.osm_id, b.name, b.osm_value, h.open_time, h.close_time
FROM transit.osm_business_hours h
JOIN transit.osm_business b ON h.osm_id = b.osm_id
WHERE h.day_of_week = :day_of_week            -- 'Monday' through 'Sunday'
  AND (:osm_value IS NULL OR b.osm_value = :osm_value)
ORDER BY b.name
LIMIT :limit
```

---

### `osm_open_now_approximate`
**Type:** availability
**Example NL:** "What coffee shops are probably open right now?" / "Pharmacies open at 9pm on a Wednesday."
_Fragile: string comparison on varchar time fields. Best-effort only. Validate in application layer._

```sql
SELECT DISTINCT b.osm_id, b.name, b.osm_value, b.lat, b.lon, h.open_time, h.close_time
FROM transit.osm_business b
JOIN transit.osm_business_hours h ON b.osm_id = h.osm_id
WHERE h.day_of_week = :day_of_week
  AND h.open_time  <= :current_hhmm
  AND (h.close_time >= :current_hhmm OR h.close_time = '24:00')
  AND (:osm_value IS NULL OR b.osm_value = :osm_value)
LIMIT :limit
```

---

### `osm_businesses_missing_hours`
**Type:** ops
**Example NL:** "Businesses with no parsed opening_hours." / "POIs we don't have hours for."

```sql
SELECT b.osm_id, b.name, b.osm_value, b.lat, b.lon
FROM transit.osm_business b
LEFT JOIN transit.osm_business_hours h ON b.osm_id = h.osm_id
WHERE h.osm_id IS NULL
ORDER BY b.name
LIMIT :limit
```

---

## 9. Cross-domain OSM + transit

### `osm_and_transit_near_point`
**Type:** cross + spatial
**Example NL:** "Citi Bike docks and cafes within 400m of Penn Station." / "Transit stops and restaurants near this location."

```sql
WITH latest AS (
  SELECT station_id, MAX(ingested_at) AS max_ts
  FROM transit.citibike_status
  GROUP BY station_id
)
SELECT 'transit'  AS source,
       l.location_id AS id, l.location_name AS name, l.location_type AS subtype,
       l.lat, l.lon,
       ST_Distance(
         ST_Point(CAST(l.lon AS DOUBLE), CAST(l.lat AS DOUBLE)),
         ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
       ) AS distance_m,
       NULL AS bikes_available
FROM transit.apitable_combined_locations l
WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL
  AND (:transit_mode IS NULL OR l.location_type = :transit_mode)
  AND ST_Distance(
        ST_Point(CAST(l.lon AS DOUBLE), CAST(l.lat AS DOUBLE)),
        ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
      ) <= :radius_m

UNION ALL

SELECT 'osm' AS source,
       b.osm_id AS id, b.name, b.osm_value AS subtype,
       b.lat, b.lon,
       ST_Distance(
         ST_Point(CAST(b.lon AS DOUBLE), CAST(b.lat AS DOUBLE)),
         ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
       ) AS distance_m,
       NULL AS bikes_available
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

---

### `osm_near_stop_open_now`
**Type:** cross
**Example NL:** "Coffee shops near this subway stop that are open right now." / "Pharmacies open now near Atlantic Terminal."
_Precondition: stop resolved to coordinates. Day/time supplied by app._

```sql
SELECT b.osm_id, b.name, b.osm_value, b.lat, b.lon,
       h.open_time, h.close_time,
       ST_Distance(
         ST_Point(CAST(b.lon AS DOUBLE), CAST(b.lat AS DOUBLE)),
         ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
       ) AS distance_m
FROM transit.osm_business b
JOIN transit.osm_business_hours h ON b.osm_id = h.osm_id
WHERE b.lat IS NOT NULL AND b.lon IS NOT NULL
  AND (:osm_value IS NULL OR b.osm_value = :osm_value)
  AND h.day_of_week = :day_of_week
  AND h.open_time  <= :current_hhmm
  AND (h.close_time >= :current_hhmm OR h.close_time = '24:00')
  AND ST_Distance(
        ST_Point(CAST(b.lon AS DOUBLE), CAST(b.lat AS DOUBLE)),
        ST_Point(CAST(:lon AS DOUBLE), CAST(:lat AS DOUBLE))
      ) <= :radius_m
ORDER BY distance_m
LIMIT :limit
```

---

### `osm_in_boundary_with_hours_on_day`
**Type:** cross + spatial
**Example NL:** "Pharmacies in Astoria with Saturday hours." / "Gyms in Brooklyn open on Sunday."

```sql
SELECT b.osm_id, b.name, b.osm_value, b.lat, b.lon, h.open_time, h.close_time
FROM transit.osm_business b
JOIN transit.geo_boundaries gb
  ON gb.boundary_type = :boundary_type
 AND gb.boundary_id   = :boundary_id
LEFT JOIN transit.osm_business_hours h
  ON b.osm_id = h.osm_id AND h.day_of_week = :day_of_week
WHERE b.lat IS NOT NULL AND b.lon IS NOT NULL
  AND (:osm_value IS NULL OR b.osm_value = :osm_value)
  AND ST_Contains(gb.geom, ST_Point(CAST(b.lon AS DOUBLE), CAST(b.lat AS DOUBLE)))
  AND h.osm_id IS NOT NULL           -- only include POIs that have hours for this day
ORDER BY b.name
LIMIT :limit
```

---

## 10. Feed provenance and ops

### `gtfs_feed_versions_list`
**Type:** ops
**Example NL:** "Which GTFS feeds are loaded?" / "What are the current feed ETags?"

```sql
SELECT feed_id, feed_type, source_url, etag, downloaded_at
FROM transit.gtfs_feed_versions
ORDER BY downloaded_at DESC
```

---

### `gtfs_feed_version_for_feed`
**Type:** ops / lookup
**Example NL:** "What version of the subway GTFS is stored?"

```sql
SELECT feed_id, feed_type, etag, downloaded_at
FROM transit.gtfs_feed_versions
WHERE feed_id = :feed_id
LIMIT 1
```

---

### `citibike_status_freshness`
**Type:** ops
**Example NL:** "How old is our Citi Bike data?" / "When was bike availability last updated?"

```sql
SELECT MIN(last_poll) AS oldest_station_poll,
       MAX(last_poll) AS newest_station_poll,
       COUNT(DISTINCT station_id) AS stations_with_data
FROM (
  SELECT station_id, MAX(ingested_at) AS last_poll
  FROM transit.citibike_status
  GROUP BY station_id
) t
```

---

### `traffic_data_freshness`
**Type:** ops
**Example NL:** "How recent is our traffic speed data?"

```sql
SELECT MAX(ingested_at) AS latest_reading,
       COUNT(DISTINCT segment_id) AS segments_with_data
FROM transit.traffic_speeds
```

---

### `alerts_data_freshness`
**Type:** ops
**Example NL:** "When were service alerts last ingested?"

```sql
SELECT feed_source,
       MAX(ingested_at) AS latest_ingested,
       COUNT(*) AS total_alerts,
       SUM(CASE WHEN is_active_now THEN 1 ELSE 0 END) AS active_count
FROM transit.service_alerts
GROUP BY feed_source
```

---

## Template summary — final catalog

| # | template_id | Type | Primary tables |
|---|-------------|------|----------------|
| 1 | `stops_list_by_mode` | list | apitable_combined_locations |
| 2 | `stops_count_by_mode` | count | apitable_combined_locations |
| 3 | `locations_count_all` | count | apitable_combined_locations |
| 4 | `stops_by_name_search` | resolution | apitable_combined_locations |
| 5 | `stops_near_point` | spatial | apitable_combined_locations |
| 6 | `stops_by_h3_cell` | spatial | apitable_combined_locations |
| 7 | `citibike_station_list` | list | citibike_stations |
| 8 | `citibike_station_count` | count | citibike_stations |
| 9 | `citibike_station_latest_status` | lookup | citibike_status, citibike_stations |
| 10 | `citibike_station_has_bikes` | availability | citibike_status |
| 11 | `citibike_station_has_docks` | availability | citibike_status |
| 12 | `citibike_empty_stations_now` | availability | citibike_status, citibike_stations |
| 13 | `citibike_full_stations_now` | availability | citibike_status, citibike_stations |
| 14 | `citibike_stations_near_point` | spatial | citibike_stations, citibike_status |
| 15 | `alerts_active_list` | list | service_alerts |
| 16 | `alerts_active_count` | count | service_alerts |
| 17 | `alert_by_id` | lookup | service_alerts |
| 18 | `alert_active_periods_by_alert` | lookup | service_alert_active_periods |
| 19 | `alerts_affecting_route` | list | service_alerts, service_alert_affected_entities |
| 20 | `alerts_affecting_stop` | list | service_alerts, service_alert_affected_entities |
| 21 | `alerts_entities_for_active` | list | service_alert_affected_entities, service_alerts |
| 22 | `alerts_by_severity` | list | service_alerts, service_alert_affected_entities |
| 23 | `highest_severity_alert_for_route` | lookup | service_alerts, service_alert_affected_entities |
| 24 | `alert_severity_summary` | count | service_alerts, service_alert_affected_entities |
| 25 | `route_metadata_lookup` | lookup | gtfs_routes |
| 26 | `route_search_by_short_name` | resolution | gtfs_routes |
| 27 | `route_geometry_by_route_id` | lookup | apitable_routegeom |
| 28 | `route_stop_connections` | network | gtfs_stop_connections |
| 29 | `routes_serving_stop` | network | gtfs_stop_connections, gtfs_routes |
| 30 | `stop_connectivity_top_outbound` | network | gtfs_stop_connections |
| 31 | `transfers_from_stop` | network | gtfs_transfers |
| 32 | `trips_for_route` | list | gtfs_trips |
| 33 | `calendar_for_service` | lookup | gtfs_calendar |
| 34 | `boundary_lookup_exact` | resolution | geo_boundaries |
| 35 | `boundary_lookup_alias` | resolution | geo_aliases |
| 36 | `boundary_name_candidates_like` | resolution | geo_boundaries |
| 37 | `locations_in_boundary` | spatial | apitable_combined_locations, geo_boundaries |
| 38 | `locations_count_in_boundary` | count | apitable_combined_locations, geo_boundaries |
| 39 | `citibike_in_boundary_with_status` | spatial | citibike_stations, citibike_status, geo_boundaries |
| 40 | `neighborhoods_in_borough` | list | geo_boundaries |
| 41 | `alerts_in_boundary_via_stops` | cross | service_alerts, service_alert_affected_entities, gtfs_stops, geo_boundaries |
| 42 | `alerts_with_route_geometry` | cross | service_alerts, service_alert_affected_entities, apitable_routegeom |
| 43 | `disrupted_stops_near_point` | cross+spatial | service_alerts, service_alert_affected_entities, gtfs_stops |
| 44 | `citibike_near_disrupted_stops` | cross+spatial | service_alert_affected_entities, gtfs_stops, citibike_stations, citibike_status |
| 45 | `routes_in_boundary` | cross+spatial | gtfs_stop_connections, gtfs_routes, gtfs_stops, geo_boundaries |
| 46 | `subway_positions_now` | availability | subway_positions |
| 47 | `bus_positions_by_line` | availability | bus_positions |
| 48 | `bus_lines_active_now` | list | bus_positions |
| 49 | `buses_near_point` | spatial | bus_positions |
| 50 | `traffic_speeds_recent_by_borough` | availability | traffic_speeds |
| 51 | `traffic_speeds_slowest_segments` | availability | traffic_speeds |
| 52 | `osm_business_count_total` | count | osm_business |
| 53 | `osm_business_by_id` | lookup | osm_business |
| 54 | `osm_business_full_detail` | lookup | osm_business, osm_business_categories, osm_business_hours |
| 55 | `osm_business_search_name` | resolution | osm_business |
| 56 | `osm_business_list_by_osm_value` | list | osm_business |
| 57 | `osm_business_list_by_display_category` | list | osm_business, osm_business_categories |
| 58 | `osm_business_count_by_type` | count | osm_business |
| 59 | `osm_category_counts` | count | osm_business_categories |
| 60 | `osm_business_near_point` | spatial | osm_business, osm_business_categories |
| 61 | `osm_business_in_boundary` | spatial | osm_business, geo_boundaries |
| 62 | `osm_business_list_by_postal_code` | list | osm_business |
| 63 | `osm_hours_for_business` | lookup | osm_business_hours |
| 64 | `osm_businesses_with_hours_on_day` | list | osm_business_hours, osm_business |
| 65 | `osm_open_now_approximate` | availability | osm_business, osm_business_hours |
| 66 | `osm_businesses_missing_hours` | ops | osm_business, osm_business_hours |
| 67 | `osm_and_transit_near_point` | cross+spatial | apitable_combined_locations, osm_business |
| 68 | `osm_near_stop_open_now` | cross | osm_business, osm_business_hours |
| 69 | `osm_in_boundary_with_hours_on_day` | cross+spatial | osm_business, geo_boundaries, osm_business_hours |
| 70 | `gtfs_feed_versions_list` | ops | gtfs_feed_versions |
| 71 | `gtfs_feed_version_for_feed` | ops | gtfs_feed_versions |
| 72 | `citibike_status_freshness` | ops | citibike_status |
| 73 | `traffic_data_freshness` | ops | traffic_speeds |
| 74 | `alerts_data_freshness` | ops | service_alerts |

**Total: 74 templates**

---

## Implementation notes

1. **Latest Citi Bike status:** always use `MAX(ingested_at)` CTE per `station_id`. The `citibike_status` table is append-only; no `CURRENT_STATUS` view exists.

2. **location_id join to alert stop_id:** `service_alert_affected_entities.stop_id` is the raw GTFS `stop_id` (no prefix). To join to `apitable_combined_locations`, you would need `concat(feed_id, '_', sae.stop_id)` — but `feed_id` is not stored in `service_alert_affected_entities`. Use `gtfs_stops` as the join intermediary (as in §6 templates) — it carries `stop_id` + `feed_id` directly.

3. **geo_boundaries CRS:** DCP datasets (nybb, nynta2020) are EPSG:2263 (NY State Plane, feet). If loaded into Databricks in this projection, wrap point construction with `ST_Transform(ST_Point(lon, lat), 'EPSG:4326', 'EPSG:2263')` before `ST_Contains`. Alternatively, reproject polygons to WGS84 at ingest time (recommended — do it once).

4. **NTA residential filter:** `nta_type = '0'` limits to residential NTAs. Set `:nta_type` optionally to include parks (`'9'`), airports (`'8'`), or cemeteries (`'7'`) for spatial containment when that's meaningful (e.g., checking if a stop is inside Prospect Park).

5. **OSM hours quality:** `open_time`/`close_time` are varchar fragments from a partial parser. The `osm_open_now_approximate` template is best-effort — complex OSM rules (e.g., `Mo-Fr 09:00-17:00; Sa 10:00-14:00`) may be parsed to a single row or dropped. Validate in application layer before surfacing to users.

6. **Bus positions cadence:** ~hourly ingestion. `:since_ts` in bus templates should default to `current_timestamp - interval 2 hours` to catch the last full poll window. Sub-hourly questions (e.g., "where was the B44 at 3:15pm?") cannot be answered from this table.

7. **`agg_*` tables:** excluded from this catalog version. All aggregate templates (hourly, daily, weekly, monthly, yearly for traffic, citibike, bus) will be added in v3 once the tables are fully populated.

8. **PageRank / walk_edges / business_connectivity:** excluded. Phase 2.
