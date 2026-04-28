import geopandas as gpd, fiona, pandas as pd

LION_GDB = r"C:\Users\BalajiI\github_projects\apacheprojects\sedonasandbox\nyc-transit\data\nyclion\lion\lion.gdb"

layer = next(l for l in fiona.listlayers(LION_GDB) if 'lion' in l.lower())
gdf   = gpd.read_file(LION_GDB, layer=layer)

if 'NonPed' in gdf.columns:
    gdf = gdf[gdf['NonPed'] != 'Y']

gdf = gdf.to_crs(epsg=4326)

RW_TYPE_MULT = {
    '1':0.90,'2':0.00,'3':0.50,'4':0.00,'5':0.30,
    '6':0.80,'7':0.70,'8':0.00,'9':0.95,'10':0.95,
    '12':0.00,'13':0.00,'14':1.00,'':0.70
}
def get_walkability(row):
    if str(row.get('TrafDir','')).strip() == 'P': return 1.00
    rw = str(row.get('RW_TYPE','') or '').strip()
    return RW_TYPE_MULT.get(rw, 0.70)

gdf['walkability_mult'] = [get_walkability(r) for r in gdf.to_dict('records')]
gdf = gdf[gdf['walkability_mult'] > 0].copy()

gdf_proj = gdf.to_crs(epsg=32618)
gdf['length_m']      = gdf_proj.geometry.length
gdf['walk_time_sec'] = gdf['length_m'] / (1.25 * gdf['walkability_mult'])
gdf['geometry_wkt']  = gdf['geometry'].simplify(0.000005).apply(lambda g: g.wkt if g else None)
gdf = gdf.dropna(subset=['geometry_wkt'])

df = gdf[['SegmentID','RW_TYPE','walkability_mult','length_m','walk_time_sec','geometry_wkt']]
df.to_parquet('lion_segments.parquet', index=False)
print(f"Saved {len(df):,} segments")