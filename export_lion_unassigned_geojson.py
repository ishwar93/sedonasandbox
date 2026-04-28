import argparse
from pathlib import Path

import fiona
import geopandas as gpd

KNOWN_RW_TYPES = {
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "12", "13", "14"
}


def pick_lion_layer(gdb_path: Path) -> str:
    layers = fiona.listlayers(gdb_path)
    if not layers:
        raise RuntimeError(f"No layers found in {gdb_path}")
    return next((name for name in layers if "lion" in name.lower()), layers[0])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export LION segments that use fallback walkability score."
    )
    parser.add_argument(
        "--gdb",
        default="data/nyclion/lion/lion.gdb",
        help="Path to LION geodatabase",
    )
    parser.add_argument(
        "--out",
        default="../nyc-transit-map/public/lion_unassigned.geojson",
        help="Output GeoJSON path",
    )
    args = parser.parse_args()

    gdb_path = Path(args.gdb)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    layer_name = pick_lion_layer(gdb_path)
    print(f"Reading layer: {layer_name}")
    gdf = gpd.read_file(gdb_path, layer=layer_name)
    print(f"Loaded rows: {len(gdf):,}")

    if "NonPed" in gdf.columns:
        gdf = gdf[gdf["NonPed"] != "Y"]
    if "FeatureTyp" in gdf.columns:
        gdf = gdf[gdf["FeatureTyp"] == "0"]
    else:
        raise RuntimeError("FeatureType column not found in LION data.")

    if "RW_TYPE" not in gdf.columns:
        raise RuntimeError("RW_TYPE column not found in LION data.")

    # "Unassigned" means RW_TYPE is blank or not in the explicit mapping list.
    rw = gdf["RW_TYPE"].fillna("").astype(str).str.strip()
    unassigned_mask = ~rw.isin(KNOWN_RW_TYPES)
    unassigned = gdf[unassigned_mask].copy()
    unassigned["RW_TYPE"] = rw[unassigned_mask]
    unassigned["walkability_status"] = "fallback_0_70"

    # Web map friendly CRS
    if unassigned.crs is not None and str(unassigned.crs).upper() != "EPSG:4326":
        unassigned = unassigned.to_crs(epsg=4326)

    keep_cols = ["SegmentID", "RW_TYPE", "TrafDir", "walkability_status", "geometry"]
    keep_cols = [c for c in keep_cols if c in unassigned.columns]
    unassigned = unassigned[keep_cols]

    print(f"Unassigned segments: {len(unassigned):,}")
    unassigned.to_file(out_path, driver="GeoJSON")
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()

