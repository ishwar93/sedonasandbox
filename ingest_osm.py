"""
ingest_osm.py — Fetch NYC businesses from OpenStreetMap via Overpass API
                and load them into the transit schema.

Writes 3 tables:
  osm_business            — one row per OSM element (node/way/relation) with a name
  osm_business_categories — junction table: one row per (business, category)
  osm_business_hours      — parsed from OSM opening_hours tag

No API key required. Overpass API is a free public service.
Re-runs are idempotent: all 3 tables are cleared before writing.

License: © OpenStreetMap contributors, ODbL — attribution required.
Source:  https://www.openstreetmap.org
"""

import os
import re
import time
from datetime import datetime

import requests
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

DATABRICKS_HOST      = os.environ['DATABRICKS_HOST']
DATABRICKS_HTTP_PATH = os.environ['DATABRICKS_HTTP_PATH']
DATABRICKS_TOKEN     = os.environ['DATABRICKS_TOKEN']

# NYC bounding box — south, west, north, east (Overpass order)
# Source: official NYC boundary coordinates
NYC_BBOX = (40.496010, -74.257159, 40.915568, -73.699215)

OVERPASS_URL = 'https://overpass-api.de/api/interpreter'

# Fetch nodes, ways, and relations for all named business-like features.
# 'out center' gives a single lat/lon centroid for polygon features.
# leisure parks/playgrounds excluded — not transit-relevant businesses.
OVERPASS_QUERY = """
[out:json][timeout:180][bbox:{s},{w},{n},{e}];
(
  nwr["amenity"]["name"];
  nwr["shop"]["name"];
  nwr["office"]["name"];
  nwr["tourism"]["name"];
  nwr["leisure"]["name"]["leisure"!~"park|garden|pitch|playground|nature_reserve|cemetery"];
);
out center tags;
""".format(s=NYC_BBOX[0], w=NYC_BBOX[1], n=NYC_BBOX[2], e=NYC_BBOX[3])

# Maps OSM primary tag values → human-readable category strings
# for osm_business_categories. Unmapped values fall back to title-cased tag value.
OSM_CATEGORY_MAP = {
    # amenity — food & drink
    'restaurant':       'Restaurants',
    'fast_food':        'Fast Food',
    'cafe':             'Coffee & Tea',
    'bar':              'Bars',
    'pub':              'Pubs',
    'food_court':       'Food Courts',
    'ice_cream':        'Ice Cream & Frozen Yogurt',
    'bakery':           'Bakeries',
    'biergarten':       'Beer Gardens',
    # amenity — health
    'pharmacy':         'Pharmacies',
    'clinic':           'Medical Clinics',
    'dentist':          'Dentists',
    'doctors':          'Doctors',
    'hospital':         'Hospitals',
    'veterinary':       'Veterinarians',
    # amenity — financial
    'bank':             'Banks & Credit Unions',
    'atm':              'ATMs',
    'bureau_de_change': 'Currency Exchange',
    # amenity — services
    'post_office':      'Post Offices',
    'library':          'Libraries',
    'gym':              'Gyms',
    'spa':              'Day Spas',
    'beauty_salon':     'Beauty & Spas',
    'hairdresser':      'Hair Salons',
    'laundry':          'Laundry Services',
    'fuel':             'Gas Stations',
    'car_wash':         'Car Wash',
    # amenity — entertainment & culture
    'cinema':           'Cinema',
    'theatre':          'Performing Arts',
    'nightclub':        'Nightlife',
    'arts_centre':      'Arts & Entertainment',
    'casino':           'Casinos',
    'stripclub':        'Adult Entertainment',
    # amenity — education
    'school':           'Schools',
    'university':       'Colleges & Universities',
    'college':          'Colleges & Universities',
    'kindergarten':     'Child Care & Day Care',
    'language_school':  'Language Schools',
    'music_school':     'Music Schools',
    # amenity — community
    'place_of_worship': 'Religious Organizations',
    'social_facility':  'Community Services',
    'marketplace':      'Markets',
    'courthouse':       'Government',
    'embassy':          'Embassies & Consulates',
    # shop — food & grocery
    'supermarket':      'Grocery',
    'convenience':      'Convenience Stores',
    'butcher':          'Butchers',
    'deli':             'Delis',
    'seafood':          'Seafood Markets',
    'alcohol':          'Beer Wine & Spirits',
    'greengrocer':      'Fruits & Vegetables',
    # shop — retail
    'clothes':          'Fashion',
    'shoes':            'Shoe Stores',
    'electronics':      'Electronics',
    'hardware':         'Hardware Stores',
    'furniture':        'Furniture Stores',
    'florist':          'Florists',
    'books':            'Bookstores',
    'gift':             'Gift Shops',
    'jewelry':          'Jewelry',
    'optician':         'Eyewear & Opticians',
    'sports':           'Sporting Goods',
    'toys':             'Toy Stores',
    'music':            'Musical Instruments & Teachers',
    'pet':              'Pet Stores',
    'bicycle':          'Bikes',
    'car':              'Car Dealers',
    'copyshop':         'Printing Services',
    'dry_cleaning':     'Dry Cleaning',
    'travel_agency':    'Travel Services',
    'mobile_phone':     'Mobile Phones',
    'computer':         'Computers',
    'stationery':       'Office Equipment',
    'variety_store':    'Dollar Stores',
    'chemist':          'Health & Beauty',
    'cosmetics':        'Cosmetics & Beauty Supply',
    'art':              'Art Supplies',
    'antiques':         'Antiques',
    'second_hand':      'Thrift Stores',
    # office
    'lawyer':           'Law Offices',
    'accountant':       'Accountants',
    'financial':        'Financial Services',
    'insurance':        'Insurance',
    'real_estate':      'Real Estate',
    'architect':        'Architects',
    'engineer':         'Engineering',
    'it':               'IT Services',
    'advertising':      'Advertising',
    'government':       'Government',
    'ngo':              'Non-Profit Organizations',
    'therapist':        'Counseling & Mental Health',
    # tourism
    'hotel':            'Hotels',
    'hostel':           'Hostels',
    'guest_house':      'Bed & Breakfast',
    'museum':           'Museums',
    'gallery':          'Art Galleries',
    'attraction':       'Attractions',
    'viewpoint':        'Scenic Viewpoints',
    'information':      'Tourist Information',
    # leisure
    'fitness_centre':   'Gyms',
    'swimming_pool':    'Swimming Pools',
    'sports_centre':    'Sports Clubs',
    'bowling_alley':    'Bowling',
    'dance':            'Dance Studios',
    'yoga':             'Yoga',
    'escape_game':      'Escape Games',
    'amusement_arcade': 'Arcades',
}

# OSM day abbreviations → full English day names
OSM_DAY_MAP = {
    'Mo': 'Monday',
    'Tu': 'Tuesday',
    'We': 'Wednesday',
    'Th': 'Thursday',
    'Fr': 'Friday',
    'Sa': 'Saturday',
    'Su': 'Sunday',
}

BATCH_SIZE = 500


# ══════════════════════════════════════════════════════════════════════════════
# Connection
# ══════════════════════════════════════════════════════════════════════════════

def get_connection():
    return sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )


# ══════════════════════════════════════════════════════════════════════════════
# Writer
# ══════════════════════════════════════════════════════════════════════════════

def write_batch(cursor, table: str, rows: list[dict]) -> int:
    """
    Batched INSERT using parameterised VALUES clause.
    Batch size auto-calculated from column count to stay under
    Databricks' 10,000 parameter limit.
    """
    if not rows:
        return 0
    cols       = list(rows[0].keys())
    col_names  = ', '.join(cols)
    batch_size = max(1, 9000 // len(cols))
    total      = 0
    for i in range(0, len(rows), batch_size):
        batch        = rows[i:i + batch_size]
        placeholders = ', '.join(f"({', '.join(['?' for _ in cols])})" for _ in batch)
        values       = [v for r in batch for v in r.values()]
        cursor.execute(
            f"INSERT INTO transit.{table} ({col_names}) VALUES {placeholders}",
            values
        )
        total += len(batch)
    return total


# ══════════════════════════════════════════════════════════════════════════════
# Parsers
# ══════════════════════════════════════════════════════════════════════════════

def get_lat_lon(element: dict) -> tuple[float, float] | None:
    """
    Extract lat/lon from an OSM element.
    Nodes carry lat/lon directly.
    Ways and relations use the 'center' key from 'out center'.
    """
    if element['type'] == 'node':
        lat = element.get('lat')
        lon = element.get('lon')
    else:
        center = element.get('center', {})
        lat = center.get('lat')
        lon = center.get('lon')
    return (float(lat), float(lon)) if lat is not None and lon is not None else None


def get_primary_tag(tags: dict) -> tuple[str, str]:
    """
    Return (osm_key, osm_value) for the highest-priority business tag.
    Priority: amenity > shop > office > tourism > leisure.
    """
    for key in ('amenity', 'shop', 'office', 'tourism', 'leisure'):
        val = tags.get(key)
        if val:
            return key, val
    return '', ''


def get_category(osm_value: str) -> str:
    """Map an OSM tag value to a human-readable category string."""
    return OSM_CATEGORY_MAP.get(
        osm_value,
        osm_value.replace('_', ' ').title()
    )


def resolve_days(day_spec: str) -> list[str]:
    """
    Convert an OSM day specification to a list of full day names.
    Handles ranges (Mo-Fr), comma lists (Mo,We,Fr), and single days (Sa).
    """
    day_order = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su']
    result    = []
    for part in day_spec.split(','):
        part = part.strip()
        if '-' in part:
            bounds = part.split('-')
            if len(bounds) == 2:
                s, e = bounds[0].strip(), bounds[1].strip()
                if s in day_order and e in day_order:
                    for d in day_order[day_order.index(s):day_order.index(e) + 1]:
                        full = OSM_DAY_MAP.get(d)
                        if full and full not in result:
                            result.append(full)
        else:
            full = OSM_DAY_MAP.get(part)
            if full and full not in result:
                result.append(full)
    return result


def parse_opening_hours(oh_str: str, osm_id: str) -> list[dict]:
    """
    Parse an OSM opening_hours string into rows for osm_business_hours.

    Handles common patterns:
      Mo-Fr 09:00-18:00
      Mo,We,Fr 08:00-20:00
      Sa-Su 10:00-16:00
      24/7

    Skips complex/conditional rules (PH off, "Jun-Aug Mo 10:00-14:00", etc.)
    that require a full OSM opening_hours parser.
    """
    if not oh_str:
        return []

    oh_str = oh_str.strip()

    if oh_str == '24/7':
        return [
            {'osm_id': osm_id, 'day_of_week': day,
             'open_time': '0:00', 'close_time': '24:00'}
            for day in OSM_DAY_MAP.values()
        ]

    rows = []
    for rule in oh_str.split(';'):
        rule = rule.strip()
        if not rule:
            continue
        m = re.match(
            r'^([A-Za-z,\-\s]+?)\s+(\d{1,2}:\d{2})-(\d{1,2}:\d{2})$',
            rule
        )
        if not m:
            continue   # skip rules we can't parse cleanly
        day_spec   = m.group(1).strip()
        open_time  = m.group(2)
        close_time = m.group(3)
        for day in resolve_days(day_spec):
            rows.append({
                'osm_id':      osm_id,
                'day_of_week': day,
                'open_time':   open_time,
                'close_time':  close_time,
            })
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# Overpass fetch
# ══════════════════════════════════════════════════════════════════════════════

def fetch_osm_elements(max_retries: int = 3) -> list[dict]:
    """
    Fetch all named businesses in NYC from the Overpass API.
    Uses POST to avoid URL length limits on large queries.
    Retries with exponential backoff on failure.
    """
    print("Fetching from Overpass API ...")
    print(f"  Endpoint: {OVERPASS_URL}")
    print(f"  Bbox:     {NYC_BBOX}")

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(
                OVERPASS_URL,
                data={'data': OVERPASS_QUERY},
                timeout=200
            )
            resp.raise_for_status()
            elements = resp.json().get('elements', [])
            print(f"  Received {len(elements):,} elements")
            return elements
        except requests.exceptions.Timeout:
            print(f"  Attempt {attempt}/{max_retries}: timeout")
        except requests.exceptions.RequestException as e:
            print(f"  Attempt {attempt}/{max_retries}: {e}")
        if attempt < max_retries:
            time.sleep(2 ** attempt)

    print("  Failed after all retries.")
    return []


# ══════════════════════════════════════════════════════════════════════════════
# Main ingestion
# ══════════════════════════════════════════════════════════════════════════════

def ingest():
    elements = fetch_osm_elements()
    if not elements:
        return

    now = datetime.utcnow().isoformat()

    buf_business   = []
    buf_categories = []
    buf_hours      = []

    total_written      = 0
    skipped_no_coords  = 0
    skipped_no_name    = 0
    skipped_no_cat     = 0
    parse_errors       = 0

    print("\nProcessing elements ...")

    with get_connection() as conn:
        with conn.cursor() as cursor:

            # ── Clear existing data (idempotent re-runs) ──────────────────────
            print("Clearing existing OSM tables ...")
            for tbl in ['osm_business_hours', 'osm_business_categories', 'osm_business']:
                try:
                    cursor.execute(f"DELETE FROM transit.{tbl} WHERE 1=1")
                    print(f"  Cleared transit.{tbl}")
                except Exception as e:
                    print(f"  Could not clear transit.{tbl}: {e}")

            print("\nIngesting ...")

            def flush():
                nonlocal total_written
                if buf_business:
                    total_written += write_batch(cursor, 'osm_business', buf_business)
                if buf_categories:
                    write_batch(cursor, 'osm_business_categories', buf_categories)
                if buf_hours:
                    write_batch(cursor, 'osm_business_hours', buf_hours)
                buf_business.clear()
                buf_categories.clear()
                buf_hours.clear()
                print(f"  {total_written:,} businesses written", end='\r')

            seen_ids = set()   # deduplicate across node/way/relation

            for el in elements:
                try:
                    tags = el.get('tags', {})

                    name = tags.get('name', '').strip()
                    if not name:
                        skipped_no_name += 1
                        continue

                    coords = get_lat_lon(el)
                    if not coords:
                        skipped_no_coords += 1
                        continue
                    lat, lon = coords

                    osm_key, osm_value = get_primary_tag(tags)
                    if not osm_value:
                        skipped_no_cat += 1
                        continue

                    osm_id = f"{el['type']}_{el['id']}"
                    if osm_id in seen_ids:
                        continue
                    seen_ids.add(osm_id)

                    # ── osm_business ──────────────────────────────────────────
                    buf_business.append({
                        'osm_id':      osm_id,
                        'osm_type':    el['type'],
                        'name':        name,
                        'osm_key':     osm_key,
                        'osm_value':   osm_value,
                        'lat':         lat,
                        'lon':         lon,
                        'address':     ' '.join(filter(None, [
                                           tags.get('addr:housenumber', ''),
                                           tags.get('addr:street', ''),
                                       ])) or None,
                        'postal_code': tags.get('addr:postcode') or None,
                        'ingested_at': now,
                    })

                    # ── osm_business_categories ───────────────────────────────
                    # Primary: mapped from osm_value
                    category = get_category(osm_value)
                    buf_categories.append({'osm_id': osm_id, 'category': category})

                    # Secondary: cuisine tag for food businesses
                    if osm_key == 'amenity' and osm_value in (
                        'restaurant', 'fast_food', 'cafe', 'pub', 'bar', 'food_court'
                    ):
                        cuisine = tags.get('cuisine', '')
                        for c in cuisine.split(';'):
                            c = c.strip().replace('_', ' ').title()
                            if c and c != category:
                                buf_categories.append({'osm_id': osm_id, 'category': c})

                    # ── osm_business_hours ────────────────────────────────────
                    buf_hours.extend(
                        parse_opening_hours(tags.get('opening_hours', ''), osm_id)
                    )

                except Exception as e:
                    parse_errors += 1
                    continue

                if len(buf_business) >= BATCH_SIZE:
                    flush()

            flush()   # final flush

    print()
    print(f"\n{'='*50}")
    print(f"OSM ingestion complete")
    print(f"{'='*50}")
    print(f"  Elements from Overpass:     {len(elements):>8,}")
    print(f"  Skipped (no name):          {skipped_no_name:>8,}")
    print(f"  Skipped (no coordinates):   {skipped_no_coords:>8,}")
    print(f"  Skipped (no category):      {skipped_no_cat:>8,}")
    print(f"  Parse errors:               {parse_errors:>8,}")
    print(f"  Businesses written:         {total_written:>8,}")
    print()
    print("  Verify in Databricks:")
    print("    SELECT COUNT(*) FROM transit.osm_business;")
    print("    SELECT osm_key, osm_value, COUNT(*) n FROM transit.osm_business")
    print("    GROUP BY osm_key, osm_value ORDER BY n DESC LIMIT 30;")
    print("    SELECT category, COUNT(*) n FROM transit.osm_business_categories")
    print("    GROUP BY category ORDER BY n DESC LIMIT 20;")
    print()
    print("  Attribution: © OpenStreetMap contributors (ODbL)")
    print("  Source: https://www.openstreetmap.org")


if __name__ == '__main__':
    start = datetime.utcnow()
    print(f"\n{'='*50}")
    print(f"OSM ingestion started: {start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"{'='*50}\n")
    ingest()
    elapsed = (datetime.utcnow() - start).seconds
    print(f"\nCompleted in {elapsed}s")
    print(f"{'='*50}\n")