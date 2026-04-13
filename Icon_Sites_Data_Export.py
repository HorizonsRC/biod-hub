"""
Icon_Sites_Data_Export.py
=========================
Queries the Biodiversity Icon Sites Contractor Data feature layer from AGOL,
processes waypoint and polyline data per icon site, and produces:

  1.  html/icon-sites/icon_sites_data.json   — loaded at runtime by each icon
      site HTML dashboard (replaces hardcoded DATA objects)
  2.  Per-site summary CSV files in the configured output directory

Run this script, then commit/push to GitHub Pages so the HTML dashboards
pick up the latest data automatically.

Usage (ArcGIS Pro Python environment):
    python Icon_Sites_Data_Export.py

Dependencies:
    arcgis, pandas  (both available in arcgispro-py3)

Configuration:
    Copy config.example.py → config.py and fill in ICON_SITES_* keys.
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from arcgis.gis import GIS
from arcgis.features import FeatureLayer

# ── Config ───────────────────────────────────────────────────────────────────
try:
    import config
except ImportError:
    sys.exit(
        "ERROR: config.py not found. Copy config.example.py to config.py "
        "and fill in your local paths."
    )

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs" / "icon-sites"
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_path = LOG_DIR / f"icon_sites_{datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
# Feature layer item ID for Biodiversity Icon Sites Contractor Data
# https://horizonsrc.maps.arcgis.com/home/item.html?id=8a6537788fc246b99547c5833659d828
CONTRACTOR_ITEM_ID = "8a6537788fc246b99547c5833659d828"
WAYPOINTS_LAYER_ID = 1   # BioD_Contractor_Data_waypoints
POLYLINES_LAYER_ID = 2   # BioD_Contractor_Data_polylines

# Financial year bounds (update each year)
FY_START = datetime(2024, 7, 1)
FY_END   = datetime(2025, 6, 30, 23, 59, 59)
FY_LABEL = "2024-25"

# Icon site names as they appear in the data (update to match field values)
ICON_SITES = {
    "te-apiti": "Te Apiti - Manawatu Gorge",
    # Add further sites here as HTML pages are developed, e.g.:
    # "bushy-park": "Bushy Park Tarapuruhi",
}

# Output paths
REPO_ROOT  = Path(__file__).parent
HTML_DIR   = REPO_ROOT / "html" / "icon-sites"
OUTPUT_DIR = Path(getattr(config, "ICON_SITES_OUTPUT_DIR", REPO_ROOT / "Outputs" / "icon-sites"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JSON_OUT = HTML_DIR / "icon_sites_data.json"

# ─────────────────────────────────────────────────────────────────────────────


def connect_gis() -> GIS:
    """Connect to the ArcGIS portal using stored Pro credentials."""
    log.info("Connecting to ArcGIS Online...")
    gis = GIS("pro")
    log.info(f"Connected as: {gis.properties.user.username}")
    return gis


def fetch_layer_as_df(gis: GIS, item_id: str, layer_id: int, where: str = "1=1") -> pd.DataFrame:
    """Query a feature layer and return its attribute table as a DataFrame."""
    item = gis.content.get(item_id)
    if item is None:
        raise ValueError(f"Could not find item {item_id} in the portal.")
    layer: FeatureLayer = item.layers[layer_id]
    log.info(f"Querying layer {layer_id} ({layer.properties.name})...")
    fset = layer.query(where=where, out_fields="*", return_geometry=False)
    df = fset.sdf
    log.info(f"  → {len(df):,} records returned")
    return df


def filter_fy(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Filter DataFrame to the current financial year using a date column."""
    if date_col not in df.columns:
        log.warning(f"Date column '{date_col}' not found — skipping FY filter.")
        return df
    df[date_col] = pd.to_datetime(df[date_col], unit="ms", errors="coerce")
    mask = (df[date_col] >= FY_START) & (df[date_col] <= FY_END)
    filtered = df[mask].copy()
    log.info(f"  → {len(filtered):,} records within FY {FY_LABEL}")
    return filtered


def safe_count(series: pd.Series, value) -> int:
    """Count occurrences of value in series, returning 0 if empty."""
    return int((series == value).sum())


# ── Te Apiti processor ───────────────────────────────────────────────────────

def process_te_apiti(wp: pd.DataFrame, pl: pd.DataFrame) -> dict:
    """
    Build the DATA object for te-apiti.html from waypoint and polyline DataFrames.

    Assumes the feature layer has been filtered to the Te Apiti site already.

    Column names below are placeholders — update once the CSV schema is confirmed.
    Refer to the field mapping comments marked TODO.
    """
    log.info("Processing Te Apiti data...")

    # ── TODO: update these column names to match the actual field schema ──────
    DATE_COL        = "created_date"   # or "EditDate", "survey_date" — confirm from CSV
    SITE_COL        = "icon_site"      # field that identifies the icon site
    ACTIVITY_COL    = "activity_type"  # activity category on waypoints
    SPECIES_COL     = "target_species" # species caught / observed
    CATCH_COL       = "catch_count"    # numeric catch count
    CONTRACTOR_COL  = "contractor"     # contractor name
    MONTH_COL       = "month"          # will be derived from date
    LINE_LENGTH_COL = "Shape__Length"  # polyline length field (metres) — AGOL default
    # ──────────────────────────────────────────────────────────────────────────

    # Derived month column (for polylines)
    if DATE_COL in pl.columns:
        pl[DATE_COL] = pd.to_datetime(pl[DATE_COL], unit="ms", errors="coerce")
        pl["month_num"] = pl[DATE_COL].dt.month
    else:
        pl["month_num"] = None

    # ── Stats ────────────────────────────────────────────────────────────────
    unique_visits = (
        wp[DATE_COL].dt.date.nunique() if DATE_COL in wp.columns else None
    )

    km_total = None
    if LINE_LENGTH_COL in pl.columns:
        km_total = round(pl[LINE_LENGTH_COL].sum() / 1000, 1)

    # Count by activity type
    act_counts = (
        wp[ACTIVITY_COL].value_counts().to_dict()
        if ACTIVITY_COL in wp.columns else {}
    )

    pest_events = act_counts.get("Pest Control", None)
    weed_events = act_counts.get("Weed Control", None)
    trap_events = act_counts.get("Trap Check", None)

    species_recorded = (
        int(wp[SPECIES_COL].nunique()) if SPECIES_COL in wp.columns else None
    )

    lead_contractor = None
    if CONTRACTOR_COL in wp.columns and not wp.empty:
        lead_contractor = wp[CONTRACTOR_COL].mode().iloc[0] if not wp[CONTRACTOR_COL].mode().empty else None

    # ── Activity type chart ──────────────────────────────────────────────────
    # These labels should match the actual values in ACTIVITY_COL
    activity_labels = ["Pest Control", "Weed Control", "Monitoring", "Revegetation", "Track Work"]
    activity_data   = [act_counts.get(lbl) for lbl in activity_labels]

    # ── Track km by month (polylines) ────────────────────────────────────────
    fy_months = [7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6]
    month_labels = ["Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar", "Apr", "May", "Jun"]
    if "month_num" in pl.columns and LINE_LENGTH_COL in pl.columns:
        km_by_month_raw = (
            pl.groupby("month_num")[LINE_LENGTH_COL].sum() / 1000
        ).to_dict()
        track_data = [round(km_by_month_raw.get(m, 0), 1) for m in fy_months]
    else:
        track_data = [None] * 12

    # ── Waypoint breakdown ───────────────────────────────────────────────────
    wp_labels = ["Trap Check", "Poison Station", "Species Record", "Weed Treatment", "Photo Point", "Other"]
    wp_data   = [act_counts.get(lbl) for lbl in wp_labels]

    # ── Pest catch summary ───────────────────────────────────────────────────
    catch_species = ["Possum", "Rat", "Stoat", "Ferret", "Cat", "Rabbit"]
    if SPECIES_COL in wp.columns and CATCH_COL in wp.columns:
        catch_grp = wp.groupby(SPECIES_COL)[CATCH_COL].sum().to_dict()
        catch_data = [int(catch_grp.get(sp, 0)) for sp in catch_species]
    else:
        catch_data = [None] * len(catch_species)

    # ── Observations count (non-catch records) ───────────────────────────────
    obs_count = safe_count(wp.get(ACTIVITY_COL, pd.Series()), "Species Record") or None

    return {
        "fy": FY_LABEL,
        "site": "Te Apiti – Manawatu Gorge",
        "generated": datetime.now().isoformat(),
        "stats": {
            "visits":  int(unique_visits) if unique_visits is not None else None,
            "km":      km_total,
            "pest":    int(pest_events)  if pest_events  is not None else None,
            "weed":    int(weed_events)  if weed_events  is not None else None,
            "traps":   int(trap_events)  if trap_events  is not None else None,
            "species": species_recorded,
        },
        "activityTypes": {
            "labels": activity_labels,
            "data":   activity_data,
        },
        "trackByMonth": {
            "labels": month_labels,
            "data":   track_data,
        },
        "waypointBreakdown": {
            "labels": wp_labels,
            "data":   wp_data,
        },
        "catchSummary": {
            "labels": catch_species,
            "data":   catch_data,
        },
        "summary": {
            "pestKm":       km_total,
            "pestVisits":   int(unique_visits) if unique_visits is not None else None,
            "weedEvents":   int(weed_events)   if weed_events   is not None else None,
            "weedSpecies":  None,   # TODO: derive from weed treatment records
            "observations": int(obs_count) if obs_count is not None else None,
            "contractor":   lead_contractor,
        },
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Icon Sites Data Export — started")
    log.info(f"Financial year: {FY_LABEL}  ({FY_START.date()} – {FY_END.date()})")
    log.info("=" * 60)

    gis = connect_gis()

    # Fetch full waypoint and polyline tables
    wp_all = fetch_layer_as_df(gis, CONTRACTOR_ITEM_ID, WAYPOINTS_LAYER_ID)
    pl_all = fetch_layer_as_df(gis, CONTRACTOR_ITEM_ID, POLYLINES_LAYER_ID)

    # Filter to FY
    # TODO: confirm the actual date field name after reviewing the CSV
    wp_all = filter_fy(wp_all, "created_date")
    pl_all = filter_fy(pl_all, "created_date")

    all_sites_data = {}

    for site_key, site_name in ICON_SITES.items():
        log.info(f"\nProcessing site: {site_name}")

        # Filter to this icon site
        # TODO: confirm the site-identifier field name from the CSV
        SITE_FIELD = "icon_site"
        wp_site = wp_all[wp_all.get(SITE_FIELD, pd.Series()) == site_name].copy() if SITE_FIELD in wp_all.columns else wp_all.copy()
        pl_site = pl_all[pl_all.get(SITE_FIELD, pd.Series()) == site_name].copy() if SITE_FIELD in pl_all.columns else pl_all.copy()

        if site_key == "te-apiti":
            site_data = process_te_apiti(wp_site, pl_site)
        else:
            log.warning(f"No processor defined for site '{site_key}' — skipping.")
            continue

        all_sites_data[site_key] = site_data

        # Write per-site CSV summary
        stats_flat = {**site_data["stats"], **site_data["summary"]}
        pd.DataFrame([stats_flat]).to_csv(
            OUTPUT_DIR / f"{site_key}_summary_{FY_LABEL.replace('-', '_')}.csv",
            index=False,
        )
        log.info(f"  ✓ CSV written for {site_name}")

    # Write combined JSON for all sites
    with open(JSON_OUT, "w", encoding="utf-8") as f:
        json.dump(all_sites_data, f, indent=2, default=str)
    log.info(f"\n✓ JSON written → {JSON_OUT}")
    log.info("Icon Sites Data Export — complete")
    log.info(f"Log: {log_path}")


if __name__ == "__main__":
    main()
