"""
Icon_Sites_Data_Export.py
=========================
Queries the Biodiversity Icon Sites Contractor Data feature layer from AGOL,
processes waypoint and polyline data per icon site, and writes the results
directly into each icon site HTML dashboard by replacing the DATA block
between marker comments.

No separate JSON file is generated — the HTML itself is updated and committed,
matching the pattern of existing manually-maintained dashboards like bushy-park.html.

Marker comments in each HTML file (must be present):
    /* ICON_SITE_DATA_START — ... */
    const DATA = { ... };
    /* ICON_SITE_DATA_END */

Usage (ArcGIS Pro Python environment — arcgispro-py3):
    python Icon_Sites_Data_Export.py

Then commit and push the updated HTML file(s) to GitHub Pages.

Configuration:
    Copy config.example.py → config.py and fill in ICON_SITES_* keys.
"""

import json
import logging
import re
import subprocess
import sys
import time
import datetime
from pathlib import Path

import requests

import pandas as pd
from arcgis.gis import GIS
from arcgis.features import FeatureLayer  # noqa: F401

# ── Config ────────────────────────────────────────────────────────────────────
try:
    import config
except ImportError:
    sys.exit(
        "ERROR: config.py not found. Copy config.example.py to config.py "
        "and fill in your local paths."
    )

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs" / "icon-sites"
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_path = LOG_DIR / f"icon_sites_{datetime.datetime.now():%Y%m%d_%H%M%S}.log"

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
# AGOL item/service identifiers — read from config.py so org-specific IDs
# are not committed to the public repository.
CONTRACTOR_ITEM_ID = getattr(config, "CONTRACTOR_ITEM_ID", None)
WAYPOINTS_LAYER_ID = getattr(config, "WAYPOINTS_LAYER_ID", 0)
POLYLINES_LAYER_ID = getattr(config, "POLYLINES_LAYER_ID", 1)

TRAP_SERVICE_URL   = getattr(config, "TRAP_SERVICE_URL", None)
TRAP_LAYER_ID      = getattr(config, "TRAP_LAYER_ID", 0)
INSP_TABLE_ID      = getattr(config, "INSP_TABLE_ID", 1)

PCO_MONITORING_URL  = getattr(config, "PCO_MONITORING_URL", None)
HRC_ICON_SITES_URL      = getattr(config, "HRC_ICON_SITES_URL", None)
TRAPNZ_RUAHINE_URL      = getattr(config, "TRAPNZ_RUAHINE_URL",
                                  "https://trap.nz/project/5105689/killcount.json")
RUAHINE_TRAPS_LAYER     = getattr(config, "RUAHINE_TRAPS_LAYER", None)
TRAPNZ_KW_ARAMAHOE_URL  = getattr(config, "TRAPNZ_KW_ARAMAHOE_URL",
                                  "https://trap.nz/project/9011553/killcount.json")
TRAPNZ_KW_OHOREA_URL    = getattr(config, "TRAPNZ_KW_OHOREA_URL",
                                  "https://trap.nz/project/9011957/killcount.json")
TRAPNZ_KW_RETARUKE_URL  = getattr(config, "TRAPNZ_KW_RETARUKE_URL",
                                  "https://trap.nz/project/8958164/killcount.json")
TRAPNZ_KW_MANGANUI_URL  = getattr(config, "TRAPNZ_KW_MANGANUI_URL",
                                  "https://trap.nz/project/9011797/killcount.json")
EBIRD_API_KEY         = getattr(config, "EBIRD_API_KEY", None)
TRAPNZ_API_KEY        = getattr(config, "TRAPNZ_API_KEY", None)
TRAPNZ_TE_APITI_NODE  = getattr(config, "TRAPNZ_TE_APITI_NODE", "20690899")
TRAPNZ_ME_URL         = getattr(config, "TRAPNZ_ME_URL",
                                "https://trap.nz/project/32658221/killcount.json")

if not CONTRACTOR_ITEM_ID or not TRAP_SERVICE_URL:
    sys.exit(
        "ERROR: CONTRACTOR_ITEM_ID and TRAP_SERVICE_URL must be set in config.py. "
        "See config.example.py for the required keys."
    )

# Path to the Kia Whārite local File Geodatabase — read from config.py
KIA_WHARITE_GDB = getattr(config, "KIA_WHARITE_GDB", None)

# PCO zone names that make up the Te Apiti / Manawatu Gorge programme
TE_APITI_PCO_WHERE = (
    "PCOName IN ("
    "'Bio Te Apiti', 'Bio Te Apiti Buffer', "
    "'Bio Te Apiti Buffer North', 'Bio Te Apiti Buffer South', "
    "'Bio Te Apiti South', 'Te Apiti Wasp Control'"
    ")"
)

# PCO zone name for the Pukaha – Mount Bruce programme
PUKAHA_PCO_WHERE = "PCOName IN ('Bio Pukaha')"

# PCO zone names for the Manawatū Estuary programme (all known casing/spacing variants)
MANAWATU_ESTUARY_PCO_WHERE = (
    "PCOName IN ("
    "'MANAWATU ESTUARIES ', 'Manawatu Estuaries ', "
    "'Manawatu estuaries ', 'Bio Manawatu Estuary'"
    ")"
)

# Financial year label used in log messages and CSV filenames
# Each site processor uses its own FY field/value for filtering.
FY_LABEL = "2024-25"  # used only for log/CSV naming; actual FY filter is per-site

# Regex that matches everything between the two marker comments (inclusive)
_DATA_BLOCK_RE = re.compile(
    r"/\* ICON_SITE_DATA_START.*?\*/.*?/\* ICON_SITE_DATA_END \*/",
    re.DOTALL,
)

# HTML file for each icon site (relative to repo root)
REPO_ROOT = Path(__file__).parent
HTML_DIR  = REPO_ROOT / "html" / "icon-sites"

ICON_SITE_HTML = {
    "te-apiti":          HTML_DIR / "te-apiti.html",
    "kia-wharite":       HTML_DIR / "kia-wharite.html",
    "manawatu-estuary":  HTML_DIR / "manawatu-estuary.html",
    "pukaha":            HTML_DIR / "pukaha.html",
    "ruahine-kiwi":      HTML_DIR / "ruahine-kiwi.html",
    # Add further sites here as they are developed:
    # "bushy-park": HTML_DIR / "bushy-park.html",
}

# Output directory for per-site summary CSVs (gitignored via Outputs/)
OUTPUT_DIR = Path(getattr(config, "ICON_SITES_OUTPUT_DIR", REPO_ROOT / "Outputs" / "icon-sites"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Cache files for Trap.NZ killcount endpoints (fallback when rate-limited)
TRAPNZ_RUAHINE_CACHE     = OUTPUT_DIR / "trapnz_ruahine_cache.json"
TRAPNZ_ME_CACHE          = OUTPUT_DIR / "trapnz_me_cache.json"
TRAPNZ_KW_ARAMAHOE_CACHE = OUTPUT_DIR / "trapnz_kw_aramahoe_cache.json"
TRAPNZ_KW_OHOREA_CACHE   = OUTPUT_DIR / "trapnz_kw_ohorea_cache.json"
TRAPNZ_KW_RETARUKE_CACHE = OUTPUT_DIR / "trapnz_kw_retaruke_cache.json"
TRAPNZ_KW_MANGANUI_CACHE = OUTPUT_DIR / "trapnz_kw_manganui_cache.json"

# ── AGOL helpers ──────────────────────────────────────────────────────────────

def connect_gis() -> GIS:
    log.info("Connecting to ArcGIS Online...")
    gis = GIS("pro")
    log.info(f"Connected as: {gis.properties.user.username}")
    return gis


def fetch_layer_as_df(gis: GIS, item_id: str, layer_id: int) -> pd.DataFrame:
    item = gis.content.get(item_id)
    if item is None:
        raise ValueError(f"Could not find AGOL item {item_id}.")
    if layer_id >= len(item.layers):
        available = [(i, lyr.properties.name) for i, lyr in enumerate(item.layers)]
        raise IndexError(
            f"Layer index {layer_id} not found on item {item_id}. "
            f"Available layers: {available}"
        )
    layer = item.layers[layer_id]
    log.info(f"Querying layer {layer_id} ({layer.properties.name})...")
    fset = layer.query(where="1=1", out_fields="*", return_geometry=False)
    df = fset.sdf
    log.info(f"  → {len(df):,} records returned")
    return df


_trapnz_last_call: float = 0.0
_TRAPNZ_MIN_GAP_S: int   = 8  # minimum seconds between any two Trap.NZ requests


def fetch_trapnz_json(url: str, cache_path: Path, max_retries: int = 2, retry_delay: int = 60) -> dict:
    """
    Fetch a Trap.NZ killcount JSON endpoint with courtesy throttling, retrying on
    429 rate-limit responses, and falling back to a local cache on failure.
    """
    global _trapnz_last_call
    gap = time.time() - _trapnz_last_call
    if gap < _TRAPNZ_MIN_GAP_S:
        wait = _TRAPNZ_MIN_GAP_S - gap
        log.info(f"  Trap.NZ courtesy delay {wait:.1f}s...")
        time.sleep(wait)

    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            _trapnz_last_call = time.time()
            resp = requests.get(url, timeout=30)
            if resp.status_code == 429:
                if attempt < max_retries:
                    log.warning(f"  Trap.NZ 429 rate limit — waiting {retry_delay}s (retry {attempt + 1}/{max_retries})...")
                    time.sleep(retry_delay)
                    continue
                last_exc = f"429 Too Many Requests (exhausted {max_retries} retries)"
                break
            resp.raise_for_status()
            payload = resp.json()
            try:
                cache_path.write_text(json.dumps(payload), encoding="utf-8")
                log.info(f"  Trap.NZ response cached → {cache_path.name}")
            except Exception as ce:
                log.warning(f"  Could not write Trap.NZ cache: {ce}")
            return payload
        except Exception as exc:
            last_exc = exc
            break

    log.warning(f"  Trap.NZ fetch failed: {last_exc}")
    if cache_path.exists():
        log.warning(f"  Falling back to cached Trap.NZ data ({cache_path.name})")
        return json.loads(cache_path.read_text(encoding="utf-8"))
    log.warning("  No Trap.NZ cache available — trap data will be empty")
    return {}


def fetch_service_url_as_df(gis: GIS, service_url: str, layer_id: int, where: str = "1=1") -> pd.DataFrame:
    """Query a FeatureServer layer or table directly by URL (no item ID required)."""
    url   = f"{service_url}/{layer_id}"
    layer = FeatureLayer(url, gis=gis)
    name  = getattr(layer.properties, "name", url.split("/")[-1])
    log.info(f"Querying service layer {layer_id} ({name}) where: {where[:80]}...")
    fset  = layer.query(where=where, out_fields="*", return_geometry=False)
    df    = fset.sdf
    log.info(f"  → {len(df):,} records returned")
    return df


def date_to_fy(dt) -> str | None:
    """Convert a datetime to a FY label string, e.g. 2024-10-01 → '24-25'."""
    try:
        if pd.isna(dt):
            return None
    except (TypeError, ValueError):
        return None
    month = dt.month
    year  = dt.year
    if month >= 7:
        return f"{str(year)[2:]}-{str(year + 1)[2:]}"
    return f"{str(year - 1)[2:]}-{str(year)[2:]}"


# ── HTML injection ────────────────────────────────────────────────────────────

def build_data_block(data: dict) -> str:
    """
    Render a JS DATA const from a Python dict, wrapped in the marker comments
    so the regex can find and replace it on subsequent runs.
    """
    js = json.dumps(data, indent=2, default=str)
    # json.dumps uses double-quotes; that's fine for a JS object literal
    return (
        "/* ICON_SITE_DATA_START — auto-updated by Icon_Sites_Data_Export.py"
        f" — last run {datetime.datetime.now():%Y-%m-%d %H:%M} */\n"
        f"const DATA = {js};\n"
        "/* ICON_SITE_DATA_END */"
    )


def inject_into_html(html_path: Path, data: dict) -> None:
    """Replace the DATA block in the HTML file with live data."""
    html = html_path.read_text(encoding="utf-8")
    if not _DATA_BLOCK_RE.search(html):
        raise ValueError(
            f"Marker comments not found in {html_path.name}. "
            "Expected /* ICON_SITE_DATA_START */ ... /* ICON_SITE_DATA_END */"
        )
    replacement = build_data_block(data)
    updated = _DATA_BLOCK_RE.sub(lambda _: replacement, html)
    html_path.write_text(updated, encoding="utf-8")
    log.info(f"  ✓ DATA block updated in {html_path.name}")

# ── Per-site processors ───────────────────────────────────────────────────────

def _fetch_trapnz_wfs_all(api_key: str, project_id: str, layer: str,
                          page_size: int = 5000) -> list:
    """Fetch all features from a Trap.NZ WFS layer, handling pagination."""
    base = f"https://io.trap.nz/geo/trapnz-projects/wfs/{api_key}/{project_id}"
    features: list = []
    start = 0
    while True:
        resp = requests.get(base, params={
            "service":     "WFS",
            "version":     "2.0.0",
            "request":     "GetFeature",
            "typeNames":   f"trapnz-projects:{layer}",
            "outputFormat": "application/json",
            "count":       page_size,
            "startIndex":  start,
        }, timeout=60)
        resp.raise_for_status()
        batch = resp.json().get("features", [])
        features.extend(batch)
        if len(batch) < page_size:
            break
        start += page_size
    return features


def process_te_apiti(wp: pd.DataFrame, pl: pd.DataFrame, gis: GIS) -> dict:
    """
    Derive the DATA object for te-apiti.html.

    Field names confirmed from CSV export of BioD_Contractor_Data feature layer
    (April 2025). Site is identified by SiteID == 'Palm05'; FY filtered by FinYr.
    Trap data is fetched directly from the Animal Pest Control layer by PCOName.
    """
    log.info("Processing Te Apiti – Manawatu Gorge...")

    # ── Confirmed field names ─────────────────────────────────────────────────
    FY_COL      = "FinYr"         # financial year string, e.g. '23-24'
    DATE_COL    = "Date"          # date of record (ISO string in AGOL export)
    SITE_COL    = "SiteID"        # site identifier
    SPECIES_COL = "SpeciesID"     # weed species name
    AGE_COL     = "Age_class"     # age class: 'A', 'J', 'S' (adult/juvenile/seedling)
    RPMP_COL    = "RPMPspecies"   # RPMP flag: 'Y' or 'N'
    CONT_COL    = "Cont_name"     # contractor name
    LEN_COL     = "Shape__Length" # polyline length in metres
    SITE_ID     = "Palm05"
    FY_VAL      = "24-25"
    TOP_N       = 5               # species shown individually; remainder → "Other"
    # ─────────────────────────────────────────────────────────────────────────

    # ── Filter by site (all years) ────────────────────────────────────────────
    if SITE_COL in wp.columns:
        wp = wp[wp[SITE_COL] == SITE_ID].copy()
    if SITE_COL in pl.columns:
        pl = pl[pl[SITE_COL] == SITE_ID].copy()

    # ── All-years summary (written to CSV) ────────────────────────────────────
    all_years_rows = []
    if FY_COL in wp.columns:
        fy_order = sorted(wp[FY_COL].dropna().unique())
        for fy in fy_order:
            wp_fy = wp[wp[FY_COL] == fy]
            pl_fy = pl[pl[FY_COL] == fy] if FY_COL in pl.columns else pl

            n_rec = len(wp_fy)
            km_fy = round(float(pl_fy[LEN_COL].sum()) / 1000) if LEN_COL in pl_fy.columns and not pl_fy.empty else None

            vis_fy = None
            if DATE_COL in wp_fy.columns and not wp_fy.empty:
                dts = pd.to_datetime(wp_fy[DATE_COL], errors="coerce")
                vis_fy = int(dts.dt.date.nunique())

            sp_fy  = int(wp_fy[SPECIES_COL].nunique()) if SPECIES_COL in wp_fy.columns and not wp_fy.empty else None
            rp_fy  = int((wp_fy[RPMP_COL].str.upper().isin(["Y", "YES"])).sum()) if RPMP_COL in wp_fy.columns and not wp_fy.empty else None
            rp_pct = round(rp_fy / n_rec * 100) if rp_fy is not None and n_rec > 0 else None

            age_fy = {"A": 0, "J": 0, "S": 0}
            if AGE_COL in wp_fy.columns and not wp_fy.empty:
                for k, v in wp_fy[AGE_COL].str.upper().value_counts().items():
                    if k in age_fy:
                        age_fy[k] = int(v)

            all_years_rows.append({
                "site":      SITE_ID,
                "fy":        fy,
                "records":   n_rec,
                "km":        km_fy,
                "visits":    vis_fy,
                "species":   sp_fy,
                "rpmp":      rp_fy,
                "rpmp_pct":  rp_pct,
                "adults":    age_fy["A"],
                "juveniles": age_fy["J"],
                "seedlings": age_fy["S"],
            })
        log.info(f"  All-years summary: {len(all_years_rows)} FY row(s) for {SITE_ID}")

    # ── Filter to HTML display FY ─────────────────────────────────────────────
    if FY_COL in wp.columns:
        wp = wp[wp[FY_COL] == FY_VAL].copy()
    if FY_COL in pl.columns:
        pl = pl[pl[FY_COL] == FY_VAL].copy()

    log.info(f"  {len(wp):,} waypoints, {len(pl):,} polylines for {SITE_ID} FY {FY_VAL}")

    # ── Parse dates ───────────────────────────────────────────────────────────
    if DATE_COL in pl.columns and not pl.empty:
        pl["_dt"]    = pd.to_datetime(pl[DATE_COL], errors="coerce")
        pl["_month"] = pl["_dt"].dt.month

    if DATE_COL in wp.columns and not wp.empty:
        wp["_dt"] = pd.to_datetime(wp[DATE_COL], errors="coerce")

    # ── Core stats ────────────────────────────────────────────────────────────
    km_total = (
        round(float(pl[LEN_COL].sum()) / 1000)
        if LEN_COL in pl.columns and not pl.empty
        else None
    )

    unique_visits = (
        int(wp["_dt"].dt.date.nunique())
        if "_dt" in wp.columns and not wp.empty
        else None
    )

    total_records = int(len(wp)) if not wp.empty else 0

    rpmp_count = (
        int((wp[RPMP_COL].str.upper().isin(["Y", "YES"])).sum())
        if RPMP_COL in wp.columns and not wp.empty
        else None
    )

    rpmp_pct = (
        round(rpmp_count / total_records * 100)
        if rpmp_count is not None and total_records > 0
        else None
    )

    unique_species = (
        int(wp[SPECIES_COL].nunique())
        if SPECIES_COL in wp.columns and not wp.empty
        else None
    )

    # ── Age class counts (field values: 'A', 'J', 'S') ───────────────────────
    age_map = {"A": 0, "J": 0, "S": 0}
    if AGE_COL in wp.columns and not wp.empty:
        for key, cnt in wp[AGE_COL].str.upper().value_counts().items():
            if key in age_map:
                age_map[key] = int(cnt)
    adult_count    = age_map["A"]
    juvenile_count = age_map["J"]
    seedling_count = age_map["S"]

    # ── Contractor ────────────────────────────────────────────────────────────
    lead_contractor = None
    if CONT_COL in wp.columns and not wp.empty:
        mode = wp[CONT_COL].mode()
        lead_contractor = str(mode.iloc[0]) if not mode.empty else None

    # ── Species composition: top N + "Other" ─────────────────────────────────
    species_labels:    list = []
    species_data:      list = []
    age_by_species:    dict = {}   # { species: {A, J, S} } for top-N
    other_species_list: list = []  # species names rolled into "Other"

    if SPECIES_COL in wp.columns and not wp.empty:
        sc  = wp[SPECIES_COL].value_counts()
        top = sc.head(TOP_N)
        species_labels = list(top.index)
        species_data   = [int(v) for v in top.values]

        # Age breakdown per top-N species
        if AGE_COL in wp.columns:
            for sp in species_labels:
                sp_rows  = wp[wp[SPECIES_COL] == sp]
                counts   = {"A": 0, "J": 0, "S": 0}
                for k, v in sp_rows[AGE_COL].str.upper().value_counts().items():
                    if k in counts:
                        counts[k] = int(v)
                age_by_species[sp] = counts

        other_total = int(sc.iloc[TOP_N:].sum())
        if other_total > 0:
            species_labels.append("Other")
            species_data.append(other_total)
            other_species_list = list(sc.iloc[TOP_N:].index)

    # ── Track km by month (FY order: Jul–Jun) ─────────────────────────────────
    fy_months    = [7, 8, 9, 10, 11, 12, 1, 2, 3, 4, 5, 6]
    month_labels = ["Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar", "Apr", "May", "Jun"]
    if "_month" in pl.columns and LEN_COL in pl.columns and not pl.empty:
        km_by_month = (pl.groupby("_month")[LEN_COL].sum() / 1000).to_dict()
        track_data  = [round(km_by_month.get(m, 0.0), 1) for m in fy_months]
    else:
        track_data = [None] * 12

    # ── Trap data (Animal Pest Control layer) ─────────────────────────────────
    CATCH_SPECIES = ["Cat", "Ferret", "Hedgehog", "Mouse", "Rabbit",
                     "Rat", "Stoat", "Possum", "Weasel"]

    trap_total:  int  = 0
    trap_types:  dict = {"labels": [], "data": []}
    trap_zones:  dict = {"labels": [], "data": []}
    trap_by_type_by_zone: dict = {}
    catches_by_species: dict = {"labels": [], "species": CATCH_SPECIES, "data": {}}

    try:
        traps = fetch_service_url_as_df(
            gis, TRAP_SERVICE_URL, TRAP_LAYER_ID, where=TE_APITI_PCO_WHERE
        )
        log.info(f"  Trap columns: {sorted(traps.columns.tolist())}")

        if not traps.empty:
            trap_total = len(traps)

            # Traps by type
            if "TrapType" in traps.columns:
                tc = traps["TrapType"].value_counts()
                trap_types = {"labels": list(tc.index), "data": [int(v) for v in tc.values]}

            # Traps by zone (PCOName)
            if "PCOName" in traps.columns:
                zc = traps["PCOName"].value_counts()
                trap_zones = {"labels": list(zc.index), "data": [int(v) for v in zc.values]}

            # Traps by type × zone — for tooltip breakdown on the type chart
            if "TrapType" in traps.columns and "PCOName" in traps.columns:
                for ttype, grp in traps.groupby("TrapType"):
                    zc = grp["PCOName"].value_counts()
                    trap_by_type_by_zone[str(ttype)] = {
                        str(k): int(v) for k, v in zc.items()
                    }

            # Inspect / catch records — pull the whole inspection table and filter client-side.
            # Relationship: Trap.GlobalID (origin PK) → Inspection.TrapParentID (FK).
            # The IN-clause approach was fragile (long URLs + GUID format quirks on
            # related tables), so it's simpler to grab the lot and filter in pandas.
            trap_ids = set(traps["GlobalID"].dropna().astype(str).str.strip("{}").str.lower().tolist()) \
                if "GlobalID" in traps.columns else set()

            try:
                insp_all = fetch_service_url_as_df(
                    gis, TRAP_SERVICE_URL, INSP_TABLE_ID, where="1=1"
                )
            except Exception as e:
                log.warning(f"    Inspection full-table query failed: {e}")
                insp_all = pd.DataFrame()

            if not insp_all.empty:
                log.info(f"  Inspection columns: {sorted(insp_all.columns.tolist())}")

                # Find the join column — try TrapParentID, then anything *ParentID*
                join_col = None
                for cand in ("TrapParentID", "ParentGlobalID", "ParentID"):
                    if cand in insp_all.columns:
                        join_col = cand
                        break
                if join_col is None:
                    pid_cols = [c for c in insp_all.columns if "parent" in c.lower()]
                    if pid_cols:
                        join_col = pid_cols[0]
                log.info(f"  Inspection join column: {join_col}")

                if join_col and trap_ids:
                    normalised = insp_all[join_col].astype(str).str.strip("{}").str.lower()
                    insp = insp_all[normalised.isin(trap_ids)].copy()
                else:
                    insp = pd.DataFrame()
                log.info(f"  Inspection records matching Te Apiti traps: {len(insp):,}")

                if not insp.empty and "created_date" in insp.columns and "SpeciesCaught" in insp.columns:
                    insp["_dt"] = pd.to_datetime(insp["created_date"], unit="ms", errors="coerce")
                    insp["_fy"] = insp["_dt"].apply(date_to_fy)

                    # Filter to actual catches (ignore blanks / "Nothing caught")
                    caught = insp[insp["SpeciesCaught"].isin(CATCH_SPECIES)].copy()
                    log.info(f"  Inspection rows with a recorded catch: {len(caught):,}")

                    if not caught.empty:
                        # Build a trap-GlobalID → PCOName lookup for the PCO tooltip
                        trap_pco_map = (
                            traps.set_index(
                                traps["GlobalID"].astype(str).str.strip("{}").str.lower()
                            )["PCOName"].to_dict()
                            if "GlobalID" in traps.columns and "PCOName" in traps.columns
                            else {}
                        )
                        norm_join = caught[join_col].astype(str).str.strip("{}").str.lower()
                        caught["_pco"] = norm_join.map(trap_pco_map)

                        pivot = (
                            caught.groupby(["_fy", "SpeciesCaught"])
                            .size()
                            .unstack(fill_value=0)
                        )
                        fy_order = sorted([fy for fy in pivot.index if fy], key=lambda s: s)
                        pivot = pivot.reindex(fy_order)
                        catches_by_species = {
                            "labels":  fy_order,
                            "species": CATCH_SPECIES,
                            "data": {
                                sp: [
                                    int(pivot[sp][fy]) if sp in pivot.columns else 0
                                    for fy in fy_order
                                ]
                                for sp in CATCH_SPECIES
                            },
                        }

                        # PCO detail for tooltip: { fy: { species: { pco: count } } }
                        pco_detail: dict = {}
                        for fy in fy_order:
                            pco_detail[fy] = {}
                            fy_caught = caught[caught["_fy"] == fy]
                            for sp in CATCH_SPECIES:
                                sp_rows = fy_caught[fy_caught["SpeciesCaught"] == sp]
                                if not sp_rows.empty:
                                    pco_counts = (
                                        sp_rows["_pco"]
                                        .dropna()
                                        .value_counts()
                                        .to_dict()
                                    )
                                    pco_detail[fy][sp] = {
                                        str(k): int(v) for k, v in pco_counts.items()
                                    }
                        catches_by_species["pcoDetail"] = pco_detail

                        totals = {sp: sum(catches_by_species["data"][sp]) for sp in CATCH_SPECIES}
                        log.info(f"  Catches by species (all FYs): {totals}")

        log.info(f"  {trap_total:,} traps across {len(trap_zones['labels'])} zones")

    except Exception as exc:
        log.warning(f"  Trap layer query failed — skipping trap data. Error: {exc}")

    # ── DoC trap data (Trap.NZ WFS — authenticated API) ───────────────────────
    import collections as _col
    doc_traps: dict = {
        "total":      None,
        "byType":     {"labels": [], "data": []},
        "catchesByFy": {},
    }
    if TRAPNZ_API_KEY and TRAPNZ_TE_APITI_NODE:
        try:
            # Trap installations → counts by type
            trap_feats = _fetch_trapnz_wfs_all(
                TRAPNZ_API_KEY, TRAPNZ_TE_APITI_NODE, "default-project-traps"
            )
            type_counts = _col.Counter(
                f["properties"].get("trap_type", "Unknown") for f in trap_feats
            )
            doc_traps["total"]  = len(trap_feats)
            doc_traps["byType"] = {
                "labels": [t for t, _ in type_counts.most_common()],
                "data":   [n for _, n in type_counts.most_common()],
            }
            log.info(f"  DoC traps: {doc_traps['total']} | {dict(type_counts)}")

            # Catch records (paginated) → catches by species per NZ financial year
            rec_feats = _fetch_trapnz_wfs_all(
                TRAPNZ_API_KEY, TRAPNZ_TE_APITI_NODE, "default-project-trap-records"
            )
            log.info(f"  DoC catch records fetched: {len(rec_feats)}")
            fy_sp: dict = _col.defaultdict(lambda: _col.defaultdict(int))
            for feat in rec_feats:
                p  = feat["properties"]
                sp = (p.get("species_caught") or "").strip()
                if not sp or sp == "None":
                    continue
                dt_str = p.get("record_date", "")
                if not dt_str:
                    continue
                dt = datetime.datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                fy = (f"{dt.year-1}-{str(dt.year)[2:]}" if dt.month < 7
                      else f"{dt.year}-{str(dt.year+1)[2:]}")
                # Normalise: all Rat variants → "Rat"; Stoat/Weasel kept separately
                if sp.startswith("Rat"):
                    sp = "Rat"
                fy_sp[fy][sp] += int(p.get("strikes") or 1)

            doc_traps["catchesByFy"] = {
                fy: dict(counts) for fy, counts in sorted(fy_sp.items())
            }
            log.info(f"  DoC catches by FY: {doc_traps['catchesByFy']}")

        except Exception as exc:
            log.warning(f"  DoC Trap.NZ WFS fetch failed — skipping. Error: {exc}")
    else:
        log.info("  TRAPNZ_API_KEY not set — skipping DoC trap data.")

    # -- PCO RTCI monitoring results --------------------------------------------------
    TE_APITI_PCO_RTCI_WHERE = (
        "Label IN ('Bio Te Apiti Buffer', 'Bio Te Apiti North', 'Bio Te Apiti South',"
        " 'Whakarongo', 'Woodville')"
    )
    RTCI_FY_FIELDS = [
        "F24_25_Monitor_Results",
        "F23_24_Monitor_Results",
        "F22_23_Monitor_Results",
        "F21_22_Monitor_Results",
        "F20_21_Monitor_Results",
        "F19_20_Monitor_Results",
        "F18_19_Monitor_Results",
    ]
    pco_rtci: dict = {"labels": [], "data": [], "years": []}
    if PCO_MONITORING_URL:
        try:
            pco_df = fetch_service_url_as_df(gis, PCO_MONITORING_URL, 0, where=TE_APITI_PCO_RTCI_WHERE)
            for _, row in pco_df.iterrows():
                label = row.get("Label")
                rtci_val, rtci_yr = None, None
                for field in RTCI_FY_FIELDS:
                    val = row.get(field)
                    if val is not None and str(val).strip() not in ("", "None", "null"):
                        try:
                            rtci_val = round(float(val), 1)
                            rtci_yr  = field.replace("F", "", 1).replace("_Monitor_Results", "").replace("_", "-")
                            break
                        except (TypeError, ValueError):
                            continue
                if rtci_val is not None:
                    pco_rtci["labels"].append(str(label))
                    pco_rtci["data"].append(rtci_val)
                    pco_rtci["years"].append(rtci_yr)
            log.info(f"  Te Apiti PCO RTCI: {list(zip(pco_rtci['labels'], pco_rtci['data'], pco_rtci['years']))}")
        except Exception as exc:
            log.warning(f"  PCO monitoring query failed -- skipping RTCI data. Error: {exc}")
    else:
        log.warning("  PCO_MONITORING_URL not set -- skipping RTCI data.")

    return {
        "fy":        FY_VAL,
        "site":      "Te Apiti \u2013 Manawatu Gorge",
        "generated": datetime.datetime.now().isoformat(),
        "stats": {
            "km":      km_total,
            "visits":  unique_visits,
            "records": total_records or None,
            "rpmp":    rpmp_count,
            "species": unique_species,
            "adults":  adult_count or None,
        },
        "speciesComp": {
            "labels":       species_labels,
            "data":         species_data,
            "ageBySpecies": age_by_species,
            "otherSpecies": other_species_list,
        },
        "trackByMonth": {
            "labels": month_labels,
            "data":   track_data,
        },
        "ageClass": {
            "labels": ["Adult", "Juvenile", "Seedling"],
            "data":   [adult_count, juvenile_count, seedling_count],
        },
        "summary": {
            "rpmpPct":      rpmp_pct,
            "kmTotal":      km_total,
            "visits":       unique_visits,
            "speciesCount": unique_species,
            "records":      total_records or None,
            "adults":       adult_count or None,
            "juveniles":    juvenile_count or None,
            "seedlings":    seedling_count or None,
            "contractor":   lead_contractor,
        },
        "traps": {
            "total":            trap_total or None,
            "byType":           trap_types,
            "byZone":           trap_zones,
            "byTypeByZone":     trap_by_type_by_zone,
            "catchesBySpecies": catches_by_species,
        },
        "docTraps": doc_traps,
        "allYears": all_years_rows,
        "pcoRtci":  pco_rtci,
    }


def process_kia_wharite() -> dict:
    """
    Read PCO treatment area RTCI data from the local Kia Whārite File GDB.

    Source: Kia_Wharite_Project.gdb / PCO_Treatment_Area_ExportFeatures
    Fields: PCOName (str), RTC (float — Residual Trap Catch Index, %)

    Multiple polygons may exist per PCO area (one per monitoring block).
    We group by PCOName and return the mean RTC rounded to one decimal place,
    sorted descending so the highest-pressure areas appear at the top of the chart.
    """
    import arcpy  # available only in arcgispro-py3 environment

    log.info("Processing Kia Whārite — PCO RTCI data from local GDB...")

    if not KIA_WHARITE_GDB:
        raise ValueError(
            "KIA_WHARITE_GDB is not set in config.py. "
            "Add it following the instructions in config.example.py."
        )

    fc = KIA_WHARITE_GDB + "\\PCO_Treatment_Area_ExportFeatures"
    fields = ["PCOName", "RTC", "Yr_Results"]

    rows = []
    with arcpy.da.SearchCursor(fc, fields) as cur:
        for row in cur:
            pco_name, rtc, yr = row
            if pco_name is not None and rtc is not None:
                try:
                    rows.append({
                        "pco": str(pco_name).strip(),
                        "rtc": float(rtc),
                        "yr":  str(yr).strip() if yr is not None else None,
                    })
                except (ValueError, TypeError):
                    log.warning(f"  Skipping row with non-numeric RTC: {row}")

    log.info(f"  Read {len(rows)} features from PCO_Treatment_Area_ExportFeatures")

    if not rows:
        log.warning("  No valid PCO/RTC rows found — returning empty data.")
        return {
            "site":      "Kia Wharite",
            "generated": datetime.datetime.now().isoformat(),
            "pcoRtci":   {"labels": [], "data": [], "years": []},
        }

    df = pd.DataFrame(rows)

    # Group by PCO: mean RTC, most recent Yr_Results, sorted highest RTC → lowest
    grouped = (
        df.groupby("pco", sort=False)
        .agg(rtc=("rtc", "mean"), yr=("yr", lambda x: x.dropna().max() or None))
        .reset_index()
        .sort_values("rtc", ascending=False)
    )

    labels = grouped["pco"].tolist()
    data   = [round(float(v), 1) for v in grouped["rtc"]]
    years  = [str(v) if v and str(v) != "None" else None for v in grouped["yr"]]

    log.info(f"  {len(labels)} PCO areas: {list(zip(labels, data, years))}")

    # ── Trap.NZ: year-to-date catches by area ────────────────────────────────
    KW_AREAS = [
        ("Aramahoe",         TRAPNZ_KW_ARAMAHOE_URL, TRAPNZ_KW_ARAMAHOE_CACHE),
        ("Ohorea",           TRAPNZ_KW_OHOREA_URL,   TRAPNZ_KW_OHOREA_CACHE),
        ("Retaruke",         TRAPNZ_KW_RETARUKE_URL,  TRAPNZ_KW_RETARUKE_CACHE),
        ("Manganui o te Ao", TRAPNZ_KW_MANGANUI_URL,  TRAPNZ_KW_MANGANUI_CACHE),
    ]
    area_names, rats_ytd, mustelids_ytd, other_ytd = [], [], [], []
    for area_name, url, cache in KW_AREAS:
        area_names.append(area_name)
        if not url:
            log.warning(f"  No Trap.NZ URL configured for {area_name} — defaulting to 0.")
            rats_ytd.append(0); mustelids_ytd.append(0); other_ytd.append(0)
            continue
        payload  = fetch_trapnz_json(url, cache)
        sp       = payload.get("catches", {}).get("year", {}).get("species", {})
        rats     = int(sp.get("Rat",      0) or 0)
        mustelid = int(sp.get("Mustelid", 0) or 0)
        other    = int((sp.get("Hedgehog", 0) or 0)
                     + (sp.get("Possum",   0) or 0)
                     + (sp.get("Other",    0) or 0))
        rats_ytd.append(rats); mustelids_ytd.append(mustelid); other_ytd.append(other)
        log.info(f"  {area_name}: Rat={rats}  Mustelid={mustelid}  Other={other}")

    trap_catches_by_area = {
        "areas":     area_names,
        "rats":      rats_ytd,
        "mustelids": mustelids_ytd,
        "other":     other_ytd,
    }

    return {
        "site":              "Kia Wharite",
        "generated":         datetime.datetime.now().isoformat(),
        "pcoRtci":           {"labels": labels, "data": data, "years": years},
        "trapCatchesByArea": trap_catches_by_area,
    }


def process_manawatu_estuary(wp: pd.DataFrame, gis: GIS) -> dict:
    """
    Derive the DATA object for manawatu-estuary.html.

    Weed data: BioD Contractor Data feature layer, SiteID='Horo34W', FY 23-24 and 24-25.
    Trap data: Animal Pest Control layer, PCOName variants for Manawatu Estuaries.
    Shows side-by-side FY comparison for both weed count and weed area charts.
    """
    log.info("Processing Manawatū Estuary...")

    SITE_COL    = "SiteID"
    FY_COL      = "FinYr"
    SPECIES_COL = "SpeciesID"
    SIZE_COL    = "Size_sqm"
    SITE_ID     = "Horo34W"
    FY_PRIMARY  = "24-25"
    FY_PRIOR    = "23-24"
    FY_CURRENT  = date_to_fy(datetime.datetime.today())
    TOP_N       = 5

    CATCH_SPECIES = ["Cat", "Ferret", "Hedgehog", "Mouse", "Rabbit",
                     "Rat", "Stoat", "Possum", "Weasel"]

    # ── Filter by site ────────────────────────────────────────────────────────
    if SITE_COL in wp.columns:
        wp = wp[wp[SITE_COL] == SITE_ID].copy()

    log.info(f"  {len(wp):,} waypoints for {SITE_ID}")

    # ── Split by FY ───────────────────────────────────────────────────────────
    wp_primary = wp[wp[FY_COL] == FY_PRIMARY].copy() if FY_COL in wp.columns else pd.DataFrame()
    wp_prior   = wp[wp[FY_COL] == FY_PRIOR].copy()   if FY_COL in wp.columns else pd.DataFrame()

    log.info(f"  {len(wp_primary):,} records for {FY_PRIMARY}, {len(wp_prior):,} for {FY_PRIOR}")

    # ── Stats ─────────────────────────────────────────────────────────────────
    records_2425  = int(len(wp_primary))      if not wp_primary.empty else None
    records_2324  = int(len(wp_prior))        if not wp_prior.empty   else None
    area_sqm_2425 = (
        int(round(float(wp_primary[SIZE_COL].sum())))
        if SIZE_COL in wp_primary.columns and not wp_primary.empty else None
    )
    species_2425  = (
        int(wp_primary[SPECIES_COL].nunique())
        if SPECIES_COL in wp_primary.columns and not wp_primary.empty else None
    )

    # ── Weed species by count ─────────────────────────────────────────────────
    sc_primary = (
        wp_primary[SPECIES_COL].value_counts()
        if SPECIES_COL in wp_primary.columns and not wp_primary.empty
        else pd.Series(dtype=int)
    )
    sc_prior = (
        wp_prior[SPECIES_COL].value_counts()
        if SPECIES_COL in wp_prior.columns and not wp_prior.empty
        else pd.Series(dtype=int)
    )

    # Order by combined count across both FYs; top N shown individually, rest → "Other"
    combined    = sc_primary.add(sc_prior, fill_value=0).sort_values(ascending=False)
    top_species = list(combined.head(TOP_N).index)
    other_species = list(combined.iloc[TOP_N:].index) if len(combined) > TOP_N else []

    chart_count_labels = top_species + (["Other"] if other_species else [])

    def get_counts(sc, top_sp, include_other):
        counts = [int(sc.get(sp, 0)) for sp in top_sp]
        if include_other:
            other_total = int(sum(sc.get(sp, 0) for sp in sc.index if sp not in top_sp))
            counts.append(other_total)
        return counts

    count_2425 = get_counts(sc_primary, top_species, bool(other_species))
    count_2324 = get_counts(sc_prior,   top_species, bool(other_species))

    # ── Weed species by category (24-25 only) ─────────────────────────────────
    WOODY_PESTS = {
        "Boneseed", "Boxthorn", "Brush Wattle", "Elaeagnus", "Gorse", "Inkweed",
        "Karo", "Poplar", "Sydney Golden Wattle", "Tree Lupin", "Yucca",
    }
    GROUND_COVER_PESTS = {
        "African Iceplant", "Agapanthus", "Arum Lily", "Caper Spurge", "Fleabane",
        "Formosa Lily", "Goat's Rue", "Japanese Holly Fern",
        "Osteospermum (African Daisy)", "Pampas Grass", "Periwinkle",
        "Senecio (Pink Ragwort)", "Stinking Iris",
    }
    CLIMBING_PESTS = {
        "Climbing Dock", "English Ivy", "Cape Ivy", "Convolvulus",
        "Everlasting Pea", "German Ivy", "Japanese Honeysuckle", "Smilax spp.",
    }

    cat_sp: dict = {"Woody Pests": {}, "Ground Cover Pests": {}, "Climbing Pests": {}}
    for sp, cnt in sc_primary.items():
        sp_str = str(sp)
        if sp_str in WOODY_PESTS:
            cat_sp["Woody Pests"][sp_str] = int(cnt)
        elif sp_str in GROUND_COVER_PESTS:
            cat_sp["Ground Cover Pests"][sp_str] = int(cnt)
        elif sp_str in CLIMBING_PESTS:
            cat_sp["Climbing Pests"][sp_str] = int(cnt)

    weed_by_category = {
        "labels": ["Woody Pests", "Ground Cover Pests", "Climbing Pests"],
        "data": [
            sum(cat_sp["Woody Pests"].values()),
            sum(cat_sp["Ground Cover Pests"].values()),
            sum(cat_sp["Climbing Pests"].values()),
        ],
        "species": {k: dict(sorted(v.items(), key=lambda x: -x[1])) for k, v in cat_sp.items()},
    }
    log.info(f"  Weed by category: { {k: d for k, d in zip(weed_by_category['labels'], weed_by_category['data'])} }")

    # ── Trap data (Animal Pest Control layer) ─────────────────────────────────
    trap_total         = 0
    trap_types         = {"labels": [], "data": []}
    catches_by_species = {"labels": [], "species": CATCH_SPECIES, "data": {}}

    try:
        traps = fetch_service_url_as_df(
            gis, TRAP_SERVICE_URL, TRAP_LAYER_ID, where=MANAWATU_ESTUARY_PCO_WHERE
        )
        log.info(f"  Trap columns: {sorted(traps.columns.tolist())}")

        if not traps.empty:
            trap_total = len(traps)

            if "TrapType" in traps.columns:
                tc = traps["TrapType"].value_counts()
                trap_types = {"labels": list(tc.index), "data": [int(v) for v in tc.values]}

            trap_ids = (
                set(traps["GlobalID"].dropna().astype(str).str.strip("{}").str.lower().tolist())
                if "GlobalID" in traps.columns else set()
            )

            try:
                insp_all = fetch_service_url_as_df(
                    gis, TRAP_SERVICE_URL, INSP_TABLE_ID, where="1=1"
                )
            except Exception as e:
                log.warning(f"    Inspection full-table query failed: {e}")
                insp_all = pd.DataFrame()

            if not insp_all.empty:
                log.info(f"  Inspection columns: {sorted(insp_all.columns.tolist())}")

                join_col = None
                for cand in ("TrapParentID", "ParentGlobalID", "ParentID"):
                    if cand in insp_all.columns:
                        join_col = cand
                        break
                if join_col is None:
                    pid_cols = [c for c in insp_all.columns if "parent" in c.lower()]
                    if pid_cols:
                        join_col = pid_cols[0]
                log.info(f"  Inspection join column: {join_col}")

                if join_col and trap_ids:
                    normalised = insp_all[join_col].astype(str).str.strip("{}").str.lower()
                    insp = insp_all[normalised.isin(trap_ids)].copy()
                else:
                    insp = pd.DataFrame()
                log.info(f"  Inspection records matching Manawatū Estuary traps: {len(insp):,}")

                if not insp.empty and "created_date" in insp.columns and "SpeciesCaught" in insp.columns:
                    insp["_dt"] = pd.to_datetime(insp["created_date"], unit="ms", errors="coerce")
                    insp["_fy"] = insp["_dt"].apply(date_to_fy)

                    caught = insp[insp["SpeciesCaught"].isin(CATCH_SPECIES)].copy()
                    log.info(f"  Inspection rows with a recorded catch: {len(caught):,}")

                    if not caught.empty:
                        pivot = (
                            caught.groupby(["_fy", "SpeciesCaught"])
                            .size()
                            .unstack(fill_value=0)
                        )
                        # Show prior + primary + current FY (deduplicated, in order)
                        _wanted = list(dict.fromkeys([FY_PRIOR, FY_PRIMARY, FY_CURRENT]))
                        desired = [fy for fy in _wanted if fy in pivot.index]
                        fy_order = desired if desired else sorted(
                            [fy for fy in pivot.index if fy]
                        )
                        pivot = pivot.reindex(fy_order)
                        catches_by_species = {
                            "labels":  fy_order,
                            "species": CATCH_SPECIES,
                            "data": {
                                sp: [
                                    int(pivot.at[fy, sp]) if sp in pivot.columns else 0
                                    for fy in fy_order
                                ]
                                for sp in CATCH_SPECIES
                            },
                        }
                        totals = {sp: sum(catches_by_species["data"][sp]) for sp in CATCH_SPECIES}
                        log.info(f"  Catches by species (display FYs): {totals}")

        log.info(f"  {trap_total:,} traps in Manawatū Estuary")

    except Exception as exc:
        log.warning(f"  Trap layer query failed — skipping trap data. Error: {exc}")

    # ── PCO Monitoring — RTCI results ─────────────────────────────────────────
    # Fields checked newest → oldest; PCOs are monitored every ~3 years so the
    # most recent non-null value may be several FYs back.
    PCO_RTCI_WHERE = "Label IN ('Waitarere', 'Himatangi', 'Coastal Foxton')"
    RTCI_FY_FIELDS = [
        "F24_25_Monitor_Results",
        "F23_24_Monitor_Results",
        "F22_23_Monitor_Results",
        "F21_22_Monitor_Results",
        "F20_21_Monitor_Results",
        "F19_20_Monitor_Results",
        "F18_19_Monitor_Results",
    ]

    pco_rtci: dict = {"labels": [], "data": [], "years": []}

    if PCO_MONITORING_URL:
        try:
            pco_df = fetch_service_url_as_df(gis, PCO_MONITORING_URL, 0, where=PCO_RTCI_WHERE)
            log.info(f"  PCO monitoring columns: {sorted(pco_df.columns.tolist())}")

            for _, row in pco_df.iterrows():
                label = row.get("Label")
                if label is None:
                    continue
                rtci_val = None
                rtci_yr  = None
                for field in RTCI_FY_FIELDS:
                    if field not in pco_df.columns:
                        continue
                    val = row.get(field)
                    try:
                        if val is not None and not pd.isna(val):
                            rtci_val = round(float(val), 1)
                            # Extract FY label: "F24_25_Monitor_Results" → "24-25"
                            rtci_yr = field.replace("F", "", 1).replace("_Monitor_Results", "").replace("_", "-")
                            break
                    except (TypeError, ValueError):
                        continue
                if rtci_val is not None:
                    pco_rtci["labels"].append(str(label))
                    pco_rtci["data"].append(rtci_val)
                    pco_rtci["years"].append(rtci_yr)

            log.info(f"  PCO RTCI results: {list(zip(pco_rtci['labels'], pco_rtci['data'], pco_rtci['years']))}")

        except Exception as exc:
            log.warning(f"  PCO monitoring query failed — skipping RTCI data. Error: {exc}")
    else:
        log.warning("  PCO_MONITORING_URL not set in config.py — skipping RTCI data.")

    # ── Bird sightings — eBird + iNaturalist (combined, native species only) ───
    # Both sources are normalised to occurrence count (number of records per
    # species) so the metric is consistent across the two platforms.
    # eBird:       each checklist entry = 1 occurrence regardless of howMany
    # iNaturalist: each research-grade observation = 1 occurrence
    # Introduced/pest species are excluded via an NZ-specific exclusion list
    # (eBird) and the introduced=false parameter (iNaturalist).
    BIRD_LAT       = -40.470
    BIRD_LNG       = 175.235
    BIRD_DIST_KM   = 10
    BIRD_BACK_DAYS = 365   # full year for iNaturalist; eBird caps at 30
    BIRD_TOP_N     = 8

    # Threatened / At Risk species per NZ Threat Classification System.
    # Any of these detected in sightings are surfaced as notable finds.
    BIRD_THREATENED = {
        "Australasian Bittern",   # Nationally Critical
        "New Zealand Dotterel",   # Nationally Critical
        "Black-billed Gull",      # Nationally Critical
        "Wrybill",                # Nationally Vulnerable
        "Black-fronted Tern",     # Nationally Vulnerable
        "New Zealand Grebe",      # At Risk – Declining
        "Brown Teal",             # At Risk – Recovering
        "Caspian Tern",           # At Risk – Declining
        "New Zealand Falcon",     # At Risk – Declining
        "Mātātā",                 # At Risk – Declining (Fernbird)
        "Fernbird",
    }

    # Scientific name → NZ common name lookup.
    # Applied when iNaturalist returns a scientific name (common_name is null for some taxa).
    SCI_TO_COMMON = {
        # Terns
        "Sterna striata":                       "White-fronted Tern",
        "Hydroprogne caspia":                   "Caspian Tern",
        "Chlidonias albostriatus":              "Black-fronted Tern",
        # Oystercatchers
        "Haematopus unicolor":                  "Variable Oystercatcher",
        "Haematopus finschi":                   "South Island Pied Oystercatcher",
        # Godwits / waders
        "Limosa lapponica":                     "Bar-tailed Godwit",
        "Calidris canutus":                     "Red Knot",
        "Calidris acuminata":                   "Sharp-tailed Sandpiper",
        "Calidris ruficollis":                  "Red-necked Stint",
        "Pluvialis fulva":                      "Pacific Golden Plover",
        "Anarhynchus frontalis":                "Wrybill",
        "Charadrius bicinctus":                 "Double-banded Plover",
        "Charadrius obscurus":                  "New Zealand Dotterel",
        # Egrets / herons
        "Egretta novaehollandiae":              "White-faced Heron",
        "Egretta garzetta":                     "Little Egret",
        "Ardea modesta":                        "White Heron",
        "Ardea alba":                           "Great Egret",
        # Swallows
        "Hirundo neoxena":                      "Welcome Swallow",
        # Gulls
        "Chroicocephalus novaehollandiae":      "Silver Gull",
        "Larus dominicanus":                    "Kelp Gull",
        "Chroicocephalus bulleri":              "Black-billed Gull",
        # Shags / cormorants
        "Phalacrocorax melanoleucos":           "Little Pied Shag",
        "Phalacrocorax sulcirostris":           "Little Black Shag",
        "Phalacrocorax varius":                 "Pied Shag",
        # Waterfowl — native
        "Anas chlorotis":                       "Brown Teal",
        "Spatula rhynchotis":                   "Australasian Shoveler",
        "Aythya novaeseelandiae":               "New Zealand Scaup",
        "Tadorna variegata":                    "Paradise Shelduck",
        "Anas gracilis":                        "Grey Teal",
        "Anas superciliosa":                    "Grey Duck",
        # Grebes
        "Poliocephalus rufopectus":             "New Zealand Grebe",
        # Gannets / spoonbills / bitterns
        "Sула serrator":                        "Australasian Gannet",
        "Sula serrator":                        "Australasian Gannet",
        "Platalea regia":                       "Royal Spoonbill",
        "Botaurus poiciloptilus":               "Australasian Bittern",
        # Rails / swamphens
        "Porphyrio melanotus":                  "Australasian Swamphen",
        # Stilts
        "Himantopus himantopus":                "Pied Stilt",
        # Kingfishers / fantails / silvereyes / honeyeaters / pigeons
        "Todiramphus sanctus":                  "Sacred Kingfisher",
        "Rhipidura fuliginosa":                 "New Zealand Fantail",
        "Zosterops lateralis":                  "Silvereye",
        "Prosthemadera novaeseelandiae":        "Tui",
        "Anthornis melanura":                   "New Zealand Bellbird",
        "Hemiphaga novaeseelandiae":            "New Zealand Pigeon",
        # Raptors
        "Circus approximans":                   "Swamp Harrier",
        "Falco novaeseelandiae":                "New Zealand Falcon",
        # Warblers
        "Gerygone igata":                       "Grey Gerygone",
    }

    # Introduced/pest birds in NZ — excluded from the chart.
    # Covers both eBird common names, iNaturalist common names, and scientific names.
    INTRODUCED_NZ = {
        # Waterfowl (common and scientific names)
        "Mallard", "Anas platyrhynchos",
        "Black Swan", "Cygnus atratus",
        "Canada Goose", "Branta canadensis",
        "Graylag Goose", "Anser anser",
        "Mute Swan", "Cygnus olor",
        "Pacific Black Duck x Mallard (hybrid)",
        "Anas superciliosa \u00d7 platyrhynchos",
        "Anas superciliosa × platyrhynchos",
        # Pigeons / doves
        "Rock Pigeon", "Feral Pigeon", "Spotted Dove", "Barbary Dove", "Laughing Dove",
        # Passerines
        "House Sparrow", "Common Starling", "European Starling", "Common Myna",
        "Yellowhammer", "Chaffinch", "Common Chaffinch",
        "European Greenfinch", "European Goldfinch",
        "Common Redpoll", "Redpoll",
        "Eurasian Skylark", "Skylark",
        "Song Thrush", "Common Blackbird", "Eurasian Blackbird",
        # Parrots
        "Eastern Rosella",
        # Raptors / owls
        "Little Owl",
        # Gallinaceous
        "California Quail", "Ring-necked Pheasant",
        # Other
        "Australian Magpie",
    }

    # Priority species for the Manawatū Estuary — surfaced first in the chart
    # regardless of occurrence count, so ecologically significant species
    # are not buried behind incidentals.
    BIRD_PRIORITY = [
        "Bar-tailed Godwit", "White-fronted Tern", "Variable Oystercatcher",
        "South Island Pied Oystercatcher", "Wrybill", "Caspian Tern",
        "Black-billed Gull", "Royal Spoonbill", "Australasian Bittern",
        "New Zealand Dotterel", "Pied Stilt", "New Zealand Grebe",
        "Brown Teal", "Australasian Shoveler", "New Zealand Scaup",
        "New Zealand Pigeon", "Tui", "New Zealand Bellbird",
        "Sacred Kingfisher", "Swamp Harrier", "New Zealand Fantail",
        "Grey Teal", "Paradise Shelduck", "New Zealand Falcon",
        "Red Knot", "Sharp-tailed Sandpiper", "Double-banded Plover",
        "Pacific Golden Plover", "Red-necked Stint",
    ]

    species_counts: dict = {}
    sources_used:   list = []

    # ── eBird ─────────────────────────────────────────────────────────────────
    if EBIRD_API_KEY:
        try:
            resp = requests.get(
                "https://api.ebird.org/v2/data/obs/geo/recent",
                params={"lat": BIRD_LAT, "lng": BIRD_LNG, "dist": BIRD_DIST_KM,
                        "maxResults": 500, "back": min(BIRD_BACK_DAYS, 30)},
                headers={"X-eBirdApiToken": EBIRD_API_KEY},
                timeout=30,
            )
            resp.raise_for_status()
            ebird_obs = resp.json()
            ebird_individuals = 0
            for o in ebird_obs:
                name = o.get("comName") or o.get("sciName") or "Unknown"
                name = SCI_TO_COMMON.get(name, name)
                if not name or name in INTRODUCED_NZ:
                    continue
                # eBird 'howMany' is the count of individuals observed on that
                # checklist entry. Missing or 'X' (presence-only) → count as 1.
                how = o.get("howMany")
                try:
                    count = int(how) if how is not None else 1
                except (TypeError, ValueError):
                    count = 1
                species_counts[name] = species_counts.get(name, 0) + count
                ebird_individuals += count
            sources_used.append("eBird")
            log.info(f"  eBird: {len(ebird_obs)} records → {ebird_individuals} individuals, "
                     f"{len(species_counts)} native species after filtering")
        except Exception as exc:
            log.warning(f"  eBird query failed: {exc}")
    else:
        log.warning("  EBIRD_API_KEY not set — skipping eBird sightings.")

    # ── iNaturalist ───────────────────────────────────────────────────────────
    try:
        d1 = (datetime.datetime.now() - datetime.timedelta(days=BIRD_BACK_DAYS)).strftime("%Y-%m-%d")
        resp = requests.get(
            "https://api.inaturalist.org/v1/observations",
            params={
                "iconic_taxa":   "Aves",
                "lat":           BIRD_LAT,
                "lng":           BIRD_LNG,
                "radius":        BIRD_DIST_KM,
                "quality_grade": "research",
                "introduced":    "false",   # native species only
                "d1":            d1,
                "per_page":      200,
                "order_by":      "observed_on",
            },
            timeout=30,
        )
        resp.raise_for_status()
        inat_obs = resp.json().get("results", [])
        for o in inat_obs:
            taxon = o.get("taxon") or {}
            cn    = (taxon.get("common_name") or {}).get("name")
            sci   = taxon.get("name") or ""
            # Prefer common name; if absent fall back through SCI_TO_COMMON lookup
            name  = cn or SCI_TO_COMMON.get(sci) or sci or "Unknown"
            if name and name not in INTRODUCED_NZ:
                species_counts[name] = species_counts.get(name, 0) + 1
        sources_used.append("iNaturalist")
        log.info(f"  iNaturalist: {len(inat_obs)} research-grade records")
    except Exception as exc:
        log.warning(f"  iNaturalist query failed: {exc}")

    # ── Notable / threatened species detected ─────────────────────────────────
    notable_sightings = sorted(
        [{"name": sp, "count": cnt}
         for sp, cnt in species_counts.items() if sp in BIRD_THREATENED],
        key=lambda x: x["count"], reverse=True,
    )
    log.info(f"  Notable sightings: {[(r['name'], r['count']) for r in notable_sightings]}")

    # ── Assemble combined result ───────────────────────────────────────────────
    bird_sightings: dict = {
        "labels": [], "data": [], "otherSpecies": [],
        "notableSightings": notable_sightings,
        "period": None, "total": None, "sources": [],
    }
    if species_counts:
        priority_set = set(BIRD_PRIORITY)
        priority_sp  = sorted(
            [(sp, cnt) for sp, cnt in species_counts.items() if sp in priority_set],
            key=lambda x: x[1], reverse=True
        )
        other_sp = sorted(
            [(sp, cnt) for sp, cnt in species_counts.items() if sp not in priority_set],
            key=lambda x: x[1], reverse=True
        )
        sorted_sp = priority_sp + other_sp
        top    = sorted_sp[:BIRD_TOP_N]
        others = sorted_sp[BIRD_TOP_N:]
        labels = [s[0] for s in top]
        data   = [s[1] for s in top]
        if others:
            labels.append("Other")
            data.append(len(others))
        bird_sightings = {
            "labels":          labels,
            "data":            data,
            "otherSpecies":    [s[0] for s in others],
            "notableSightings": notable_sightings,
            "period":          "Past 12 months",
            "total":           sum(species_counts.values()),
            "sources":         sources_used,
        }
        log.info(f"  Combined bird sightings top species: {list(zip(labels[:5], data[:5]))}")

    # ── Trap.NZ public killcount (Manawatū Estuary community project) ──────────
    # "year" = current calendar year catches; mapped to current NZ FY as a YTD approximation.
    trapnz_me_catches: dict = {}
    trapnz_me_traps: int   = 0
    trapnz_me_fy = date_to_fy(datetime.datetime.today())
    if TRAPNZ_ME_URL:
        kc = fetch_trapnz_json(TRAPNZ_ME_URL, TRAPNZ_ME_CACHE)
        # Structure: catches.year.species = {Rat: 76, Possum: 45, ...}
        species_data = kc.get("catches", {}).get("year", {}).get("species", {})
        trapnz_me_traps = int(kc.get("traps") or 0)
        log.info(f"  Trap.NZ ME traps: {trapnz_me_traps}, year species: {species_data}")
        for name, val in species_data.items():
            if name and val and int(val) > 0:
                trapnz_me_catches[name] = int(val)
        log.info(f"  Trap.NZ ME catches (FY {trapnz_me_fy}): {trapnz_me_catches}")

    return {
        "fy":        FY_PRIMARY,
        "site":      "Manaw\u0101t\u016b Estuary",
        "generated": datetime.datetime.now().isoformat(),
        "stats": {
            "records_2425": records_2425,
            "records_2324": records_2324,
            "area_sqm_2425": area_sqm_2425,
            "species_2425":  species_2425,
            "trapTotal":     trap_total or None,
        },
        "weedByCount": {
            "labels":      chart_count_labels,
            "data_2425":   count_2425,
            "data_2324":   count_2324,
            "otherSpecies": other_species,
        },
        "weedByCategory":  weed_by_category,
        "pcoRtci":         pco_rtci,
        "birdSightings": bird_sightings,
        "traps": {
            "total":            trap_total or None,
            "byType":           trap_types,
            "catchesBySpecies": catches_by_species,
        },
        "trapnzCatches": {
            "bySpecies":     trapnz_me_catches,
            "trapsDeployed": trapnz_me_traps if TRAPNZ_ME_URL else 0,
        },
    }


def process_pukaha(wp: pd.DataFrame, pl: pd.DataFrame, gis: GIS) -> dict:
    """
    Derive the DATA object for pukaha.html.

    Weed data: BioD Contractor Data feature layer, SiteName='Pukaha extension', FY 24-25.
    Trap data: Animal Pest Control layer, PCOName IN ('Bio Pukaha').
    Filtered by SiteName rather than SiteID (field confirmed against layer schema on first run).
    """
    log.info("Processing Pukaha – Mount Bruce...")

    SITE_NAME_COL = "SiteName"
    FY_COL        = "FinYr"
    DATE_COL      = "Date"
    SPECIES_COL   = "SpeciesID"
    AGE_COL       = "Age_class"
    RPMP_COL      = "RPMPspecies"
    SIZE_COL      = "Size_sqm"
    LEN_COL       = "Shape__Length"
    SITE_NAME     = "Pukaha extension"
    FY_VAL        = "24-25"
    TOP_N         = 5

    CATCH_SPECIES = ["Cat", "Ferret", "Hedgehog", "Mouse", "Rabbit",
                     "Rat", "Stoat", "Possum", "Weasel"]

    # Species excluded from the composition chart and species count.
    # One-off misidentifications / native Clematis confusion noted in OMB report.
    PUKAHA_EXCLUDE_SPECIES = {
        "Clematis spp.", "Clematis", "Darwin's Barberry", "Berberis darwinii",
    }

    # ── Filter by site name ───────────────────────────────────────────────────
    if SITE_NAME_COL in wp.columns:
        wp = wp[wp[SITE_NAME_COL] == SITE_NAME].copy()
    else:
        log.warning(f"  Column '{SITE_NAME_COL}' not found in waypoints — check layer schema")
    if SITE_NAME_COL in pl.columns:
        pl = pl[pl[SITE_NAME_COL] == SITE_NAME].copy()

    log.info(f"  {len(wp):,} waypoints, {len(pl):,} polylines for {SITE_NAME!r}")

    # Keep site-filtered copies for multi-year by-FY charts (before FY filter below)
    wp_site = wp.copy()
    pl_site = pl.copy()

    # ── Filter to display FY ──────────────────────────────────────────────────
    if FY_COL in wp.columns:
        wp = wp[wp[FY_COL] == FY_VAL].copy()
    if FY_COL in pl.columns:
        pl = pl[pl[FY_COL] == FY_VAL].copy()

    log.info(f"  {len(wp):,} waypoints, {len(pl):,} polylines for FY {FY_VAL}")

    # ── Parse dates ───────────────────────────────────────────────────────────
    if DATE_COL in pl.columns and not pl.empty:
        pl["_dt"]    = pd.to_datetime(pl[DATE_COL], errors="coerce")
        pl["_month"] = pl["_dt"].dt.month
    if DATE_COL in wp.columns and not wp.empty:
        wp["_dt"] = pd.to_datetime(wp[DATE_COL], errors="coerce")

    # ── Core stats ────────────────────────────────────────────────────────────
    km_total = (
        round(float(pl[LEN_COL].sum()) / 1000)
        if LEN_COL in pl.columns and not pl.empty else None
    )
    unique_visits = (
        int(wp["_dt"].dt.date.nunique())
        if "_dt" in wp.columns and not wp.empty else None
    )
    total_records  = int(len(wp)) if not wp.empty else 0
    unique_species = (
        int(wp[~wp[SPECIES_COL].isin(PUKAHA_EXCLUDE_SPECIES)][SPECIES_COL].nunique())
        if SPECIES_COL in wp.columns and not wp.empty else None
    )
    rpmp_count = (
        int((wp[RPMP_COL].str.upper().isin(["Y", "YES"])).sum())
        if RPMP_COL in wp.columns and not wp.empty else None
    )
    rpmp_pct = (
        round(rpmp_count / total_records * 100)
        if rpmp_count is not None and total_records > 0 else None
    )
    area_sqm = (
        int(round(float(wp[SIZE_COL].sum())))
        if SIZE_COL in wp.columns and not wp.empty and wp[SIZE_COL].notna().any() else None
    )

    # ── Age class counts ──────────────────────────────────────────────────────
    age_map = {"A": 0, "J": 0, "S": 0}
    if AGE_COL in wp.columns and not wp.empty:
        for key, cnt in wp[AGE_COL].str.upper().value_counts().items():
            if key in age_map:
                age_map[key] = int(cnt)

    # ── Species composition: top N + "Other" ─────────────────────────────────
    species_labels:    list = []
    species_data:      list = []
    other_species_list: list = []

    if SPECIES_COL in wp.columns and not wp.empty:
        sc  = wp[~wp[SPECIES_COL].isin(PUKAHA_EXCLUDE_SPECIES)][SPECIES_COL].value_counts()
        top = sc.head(TOP_N)
        species_labels = list(top.index)
        species_data   = [int(v) for v in top.values]
        other_total    = int(sc.iloc[TOP_N:].sum())
        if other_total > 0:
            species_labels.append("Other")
            species_data.append(other_total)
            other_species_list = list(sc.iloc[TOP_N:].index)

    # ── Track coverage by type × FY (all FYs, from pl_site) ─────────────────
    # Notes field values set by user distinguish helicopter survey from ground
    # control tracks. Anything matching HELI_KEYWORDS → helicopter; rest → ground.
    NOTES_COL     = "Notes"
    HELI_KEYWORDS = ["helicopter", "heli", "aerial"]
    track_by_type: dict = {"labels": [], "helicopter": [], "ground": []}

    if FY_COL in pl_site.columns and LEN_COL in pl_site.columns and not pl_site.empty:
        if NOTES_COL in pl_site.columns:
            unique_notes = pl_site[NOTES_COL].dropna().astype(str).unique()
            log.info(f"  Polyline Notes values: {list(unique_notes)}")
            notes_lower = pl_site[NOTES_COL].fillna("").astype(str).str.lower()
            is_heli = notes_lower.str.contains("|".join(HELI_KEYWORDS), na=False)
        else:
            log.warning(f"  '{NOTES_COL}' not found in polylines — all km counted as ground")
            is_heli = pd.Series([False] * len(pl_site), index=pl_site.index)

        all_fy = sorted(pl_site[FY_COL].dropna().unique())
        heli_km, ground_km = [], []
        for fy in all_fy:
            fy_mask = pl_site[FY_COL] == fy
            heli_km.append(round(float(pl_site[fy_mask & is_heli][LEN_COL].sum()) / 1000, 1))
            ground_km.append(round(float(pl_site[fy_mask & ~is_heli][LEN_COL].sum()) / 1000, 1))
        track_by_type = {"labels": list(all_fy), "helicopter": heli_km, "ground": ground_km}
        log.info(f"  Track by type: { {fy: (h, g) for fy, h, g in zip(all_fy, heli_km, ground_km)} }")

    # ── PCO RTCI monitoring results ───────────────────────────────────────────
    PUKAHA_PCO_RTCI_WHERE = "Label IN ('Eketahuna', 'Eketahuna South', 'Tararua Ground')"
    RTCI_FY_FIELDS = [
        "F24_25_Monitor_Results", "F23_24_Monitor_Results", "F22_23_Monitor_Results",
        "F21_22_Monitor_Results", "F20_21_Monitor_Results", "F19_20_Monitor_Results",
        "F18_19_Monitor_Results",
    ]
    pco_rtci: dict = {"labels": [], "data": [], "years": []}

    if PCO_MONITORING_URL:
        try:
            pco_df = fetch_service_url_as_df(gis, PCO_MONITORING_URL, 0, where=PUKAHA_PCO_RTCI_WHERE)
            log.info(f"  PCO monitoring columns: {sorted(pco_df.columns.tolist())}")
            for _, row in pco_df.iterrows():
                label = row.get("Label")
                if label is None:
                    continue
                rtci_val, rtci_yr = None, None
                for field in RTCI_FY_FIELDS:
                    if field not in pco_df.columns:
                        continue
                    val = row.get(field)
                    try:
                        if val is not None and not pd.isna(val):
                            rtci_val = round(float(val), 1)
                            rtci_yr  = field.replace("F", "", 1).replace("_Monitor_Results", "").replace("_", "-")
                            break
                    except (TypeError, ValueError):
                        continue
                if rtci_val is not None:
                    pco_rtci["labels"].append(str(label))
                    pco_rtci["data"].append(rtci_val)
                    pco_rtci["years"].append(rtci_yr)
            log.info(f"  Pukaha PCO RTCI: {list(zip(pco_rtci['labels'], pco_rtci['data'], pco_rtci['years']))}")
        except Exception as exc:
            log.warning(f"  PCO monitoring query failed — skipping RTCI data. Error: {exc}")
    else:
        log.warning("  PCO_MONITORING_URL not set — skipping RTCI data.")

    # ── Trap data (Animal Pest Control layer) ─────────────────────────────────
    trap_total  = 0
    trap_types  = {"labels": [], "data": []}
    catches_by_species = {"labels": [], "species": CATCH_SPECIES, "data": {}}

    try:
        traps = fetch_service_url_as_df(
            gis, TRAP_SERVICE_URL, TRAP_LAYER_ID, where=PUKAHA_PCO_WHERE
        )
        log.info(f"  Trap columns: {sorted(traps.columns.tolist())}")

        if not traps.empty:
            trap_total = len(traps)

            if "TrapType" in traps.columns:
                tc = traps["TrapType"].value_counts()
                trap_types = {"labels": list(tc.index), "data": [int(v) for v in tc.values]}

            trap_ids = (
                set(traps["GlobalID"].dropna().astype(str).str.strip("{}").str.lower().tolist())
                if "GlobalID" in traps.columns else set()
            )

            try:
                insp_all = fetch_service_url_as_df(
                    gis, TRAP_SERVICE_URL, INSP_TABLE_ID, where="1=1"
                )
            except Exception as e:
                log.warning(f"    Inspection full-table query failed: {e}")
                insp_all = pd.DataFrame()

            if not insp_all.empty:
                join_col = None
                for cand in ("TrapParentID", "ParentGlobalID", "ParentID"):
                    if cand in insp_all.columns:
                        join_col = cand
                        break
                if join_col is None:
                    pid_cols = [c for c in insp_all.columns if "parent" in c.lower()]
                    if pid_cols:
                        join_col = pid_cols[0]
                log.info(f"  Inspection join column: {join_col}")

                if join_col and trap_ids:
                    normalised = insp_all[join_col].astype(str).str.strip("{}").str.lower()
                    insp = insp_all[normalised.isin(trap_ids)].copy()
                else:
                    insp = pd.DataFrame()
                log.info(f"  Inspection records matching Pukaha traps: {len(insp):,}")

                if not insp.empty and "created_date" in insp.columns and "SpeciesCaught" in insp.columns:
                    insp["_dt"] = pd.to_datetime(insp["created_date"], unit="ms", errors="coerce")
                    insp["_fy"] = insp["_dt"].apply(date_to_fy)

                    caught = insp[insp["SpeciesCaught"].isin(CATCH_SPECIES)].copy()
                    log.info(f"  Inspection rows with a recorded catch: {len(caught):,}")

                    if not caught.empty:
                        pivot = (
                            caught.groupby(["_fy", "SpeciesCaught"])
                            .size()
                            .unstack(fill_value=0)
                        )
                        fy_order = sorted([fy for fy in pivot.index if fy])
                        pivot    = pivot.reindex(fy_order)
                        catches_by_species = {
                            "labels":  fy_order,
                            "species": CATCH_SPECIES,
                            "data": {
                                sp: [int(pivot.at[fy, sp]) if sp in pivot.columns else 0
                                     for fy in fy_order]
                                for sp in CATCH_SPECIES
                            },
                        }
                        totals = {sp: sum(catches_by_species["data"][sp]) for sp in CATCH_SPECIES}
                        log.info(f"  Catches by species (all FYs): {totals}")

        log.info(f"  {trap_total:,} traps in Bio Pukaha")

    except Exception as exc:
        log.warning(f"  Trap layer query failed — skipping trap data. Error: {exc}")

    # ── OMB controlled by FY × age class (all FYs) ───────────────────────────
    # Uses wp_site (all FYs) so the chart shows the full programme history.
    # Filtered to Control_notes not-null: records where a control action was
    # recorded (cut stump / foliar spray / hedge trim) vs survey-only visits.
    omb_by_fy_age: dict = {"labels": [], "adults": [], "juveniles": [], "seedlings": []}

    CONTROL_COL = "Control_notes"
    OMB_SPECIES = "Old man's beard"

    if FY_COL in wp_site.columns and SPECIES_COL in wp_site.columns and not wp_site.empty:
        omb = wp_site[wp_site[SPECIES_COL] == OMB_SPECIES].copy()
        if CONTROL_COL in omb.columns:
            omb = omb[omb[CONTROL_COL].notna() & (omb[CONTROL_COL].astype(str).str.strip() != "")]
            log.info(f"  OMB Control_notes values: {omb[CONTROL_COL].value_counts().to_dict()}")
        else:
            log.warning(f"  Column '{CONTROL_COL}' not found — using all OMB records")

        if not omb.empty and AGE_COL in omb.columns and FY_COL in omb.columns:
            all_fy = sorted(omb[FY_COL].dropna().unique())
            adults, juveniles, seedlings = [], [], []
            for fy in all_fy:
                ac = omb[omb[FY_COL] == fy][AGE_COL].str.upper().value_counts()
                adults.append(int(ac.get("A", 0)))
                juveniles.append(int(ac.get("J", 0)))
                seedlings.append(int(ac.get("S", 0)))
            omb_by_fy_age = {
                "labels":    list(all_fy),
                "adults":    adults,
                "juveniles": juveniles,
                "seedlings": seedlings,
            }
            log.info(f"  OMB by FY×age: { {fy: (a,j,s) for fy,a,j,s in zip(all_fy,adults,juveniles,seedlings)} }")

    # ── Reserve and buffer zone polygon areas ─────────────────────────────────
    PUKAHA_AREAS_WHERE = "SiteName IN ('Pūkaha Mt Bruce Northern Buffer', 'Pūkaha Mt Bruce')"
    AREA_NAME_COL      = "SiteName"
    AREA_HA_COL        = "Hectares"
    buffer_ha  = None
    reserve_ha = None

    try:
        areas_df = fetch_service_url_as_df(gis, HRC_ICON_SITES_URL, 0, where=PUKAHA_AREAS_WHERE)
        log.info(f"  Pukaha areas columns: {sorted(areas_df.columns.tolist())}")
        if AREA_HA_COL in areas_df.columns and AREA_NAME_COL in areas_df.columns:
            for _, row in areas_df.iterrows():
                name = str(row.get(AREA_NAME_COL, ""))
                raw  = row.get(AREA_HA_COL)
                try:
                    ha = round(float(raw), 1) if raw is not None else None
                except (TypeError, ValueError):
                    ha = None
                if "Northern Buffer" in name:
                    buffer_ha = ha
                elif name.strip() == "Pūkaha Mt Bruce":
                    reserve_ha = ha
        log.info(f"  Pukaha reserve_ha={reserve_ha}, buffer_ha={buffer_ha}")
    except Exception as exc:
        log.warning(f"  Pukaha polygon areas query failed — skipping. Error: {exc}")

    return {
        "fy":        FY_VAL,
        "site":      "Pukaha \u2013 Mount Bruce",
        "generated": datetime.datetime.now().isoformat(),
        "stats": {
            "km":        km_total,
            "visits":    unique_visits,
            "records":   total_records or None,
            "rpmp":      rpmp_count,
            "species":   unique_species,
            "trapTotal": trap_total or None,
            "areaSqm":   area_sqm,
            "bufferHa":  buffer_ha,
            "reserveHa": reserve_ha,
        },
        "speciesComp": {
            "labels":       species_labels,
            "data":         species_data,
            "otherSpecies": other_species_list,
        },
        "trackByType": track_by_type,
        "summary": {
            "rpmpPct":      rpmp_pct,
            "kmTotal":      km_total,
            "visits":       unique_visits,
            "speciesCount": unique_species,
            "records":      total_records or None,
        },
        "traps": {
            "total":            trap_total or None,
            "byType":           trap_types,
            "catchesBySpecies": catches_by_species,
        },
        "ombByFyAge": omb_by_fy_age,
        "pcoRtci":    pco_rtci,
    }


# ── Ruahine Kiwi ──────────────────────────────────────────────────────────────

def process_ruahine_kiwi(gis) -> dict:
    """
    Derive the DATA object for ruahine-kiwi.html.

    Primary data source: Trap.NZ public project killcount endpoint (no auth required).
    Project: Ruahine Kiwi Project (ID 5105689), a partnership between
    Te Kāuru and Environment Network Manawatū.
    """
    log.info("Processing Ruahine Kiwi Project...")

    SPECIES_ORDER = ["Rat", "Possum", "Mustelid", "Hedgehog", "Other"]

    trapnz: dict = {
        "traps":              None,
        "monitoringStations": None,
        "installations":      None,
        "catchesAll":         None,
        "catchesYear":        None,
        "catchesMonth":       None,
        "catchesBySpecies": {
            "labels": SPECIES_ORDER,
            "all":    [],
            "year":   [],
            "month":  [],
        },
    }

    tz_data = fetch_trapnz_json(TRAPNZ_RUAHINE_URL, TRAPNZ_RUAHINE_CACHE)
    if tz_data:
        trapnz["traps"]              = tz_data.get("traps")
        trapnz["monitoringStations"] = tz_data.get("monitoring_stations")
        trapnz["installations"]      = tz_data.get("installations")

        catches = tz_data.get("catches", {})
        for period in ("all", "year", "month"):
            c  = catches.get(period, {})
            sp = c.get("species", {})
            if period == "all":
                trapnz["catchesAll"]   = c.get("total")
            elif period == "year":
                trapnz["catchesYear"]  = c.get("total")
            elif period == "month":
                trapnz["catchesMonth"] = c.get("total")
            trapnz["catchesBySpecies"][period] = [sp.get(s, 0) for s in SPECIES_ORDER]

        log.info(
            f"  Trap.NZ: {trapnz['traps']} traps, "
            f"{trapnz['catchesAll']} all-time catches, "
            f"{trapnz['catchesYear']} this year"
        )

    # ── PCO RTCI monitoring results (Horizons-managed zones) ─────────────────
    RUAHINE_PCO_RTCI_WHERE = (
        "Label IN ('Apiti', 'Matamau West', 'Norsewood', 'Ruaroa', 'South Mokai', 'Umutoi')"
    )
    RTCI_FY_FIELDS = [
        "F24_25_Monitor_Results", "F23_24_Monitor_Results", "F22_23_Monitor_Results",
        "F21_22_Monitor_Results", "F20_21_Monitor_Results", "F19_20_Monitor_Results",
        "F18_19_Monitor_Results",
    ]
    pco_rtci: dict = {"labels": [], "data": [], "years": []}

    if PCO_MONITORING_URL:
        try:
            pco_df = fetch_service_url_as_df(gis, PCO_MONITORING_URL, 0, where=RUAHINE_PCO_RTCI_WHERE)
            log.info(f"  PCO monitoring columns: {sorted(pco_df.columns.tolist())}")
            for _, row in pco_df.iterrows():
                label = row.get("Label")
                if label is None:
                    continue
                rtci_val, rtci_yr = None, None
                for field in RTCI_FY_FIELDS:
                    if field not in pco_df.columns:
                        continue
                    val = row.get(field)
                    try:
                        if val is not None and not pd.isna(val):
                            rtci_val = round(float(val), 1)
                            rtci_yr  = field.replace("F", "", 1).replace("_Monitor_Results", "").replace("_", "-")
                            break
                    except (TypeError, ValueError):
                        continue
                if rtci_val is not None:
                    pco_rtci["labels"].append(str(label))
                    pco_rtci["data"].append(rtci_val)
                    pco_rtci["years"].append(rtci_yr)
            log.info(f"  Ruahine PCO RTCI: {list(zip(pco_rtci['labels'], pco_rtci['data'], pco_rtci['years']))}")
        except Exception as exc:
            log.warning(f"  PCO monitoring query failed — skipping RTCI data. Error: {exc}")
    else:
        log.warning("  PCO_MONITORING_URL not set — skipping RTCI data.")

    # ── Trap network by type (local GDB export from Trap.NZ) ─────────────────
    traps_by_type: dict = {"labels": [], "data": []}

    if RUAHINE_TRAPS_LAYER:
        try:
            import arcpy
            import collections as _collections
            counts = _collections.Counter()
            with arcpy.da.SearchCursor(RUAHINE_TRAPS_LAYER, ["Trap_type"]) as cur:
                for (trap_type,) in cur:
                    counts[trap_type or "Unknown"] += 1
            ordered = counts.most_common()
            traps_by_type = {
                "labels": [t for t, _ in ordered],
                "data":   [c for _, c in ordered],
            }
            log.info(f"  Trap types: {dict(ordered)}")
        except Exception as exc:
            log.warning(f"  Ruahine trap GDB query failed — skipping. Error: {exc}")
    else:
        log.warning("  RUAHINE_TRAPS_LAYER not set — skipping trap type breakdown.")

    return {
        "fy":          "24-25",
        "site":        "Ruahine Kiwi Project",
        "generated":   datetime.datetime.now().isoformat(),
        "trapnz":      trapnz,
        "trapsByType": traps_by_type,
        "pcoRtci":     pco_rtci,
    }


# ── Git push ──────────────────────────────────────────────────────────────────

def git_commit_and_push(html_paths: list[Path]) -> None:
    """Stage updated HTML files, commit, and push to origin."""
    rel_paths = [str(p.relative_to(REPO_ROOT)).replace("\\", "/") for p in html_paths]
    log.info(f"Git: staging {rel_paths}...")
    subprocess.run(["git", "add"] + rel_paths, cwd=REPO_ROOT, check=True)
    msg = (
        f"Auto-update icon site HTML data blocks — FY {FY_LABEL}\n\n"
        f"Updated by Icon_Sites_Data_Export.py on {datetime.datetime.now():%Y-%m-%d %H:%M}"
    )
    result = subprocess.run(
        ["git", "diff", "--cached", "--quiet"], cwd=REPO_ROOT
    )
    if result.returncode == 0:
        log.info("Git: nothing to commit — HTML data blocks unchanged.")
        return
    subprocess.run(["git", "commit", "-m", msg], cwd=REPO_ROOT, check=True)
    log.info("Git: pulling remote changes before push...")
    subprocess.run(["git", "pull", "--rebase", "--autostash", "origin", "HEAD"], cwd=REPO_ROOT, check=True)
    log.info("Git: pushing to origin...")
    subprocess.run(["git", "push", "--set-upstream", "origin", "HEAD"], cwd=REPO_ROOT, check=True)
    log.info("Git: push complete.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Icon Sites Data Export — started")
    log.info("=" * 60)

    gis = connect_gis()

    # Fetch full attribute tables (geometry not needed).
    # Each site processor applies its own site and FY filters.
    wp_all = fetch_layer_as_df(gis, CONTRACTOR_ITEM_ID, WAYPOINTS_LAYER_ID)
    pl_all = fetch_layer_as_df(gis, CONTRACTOR_ITEM_ID, POLYLINES_LAYER_ID)

    updated_html: list[Path] = []

    for site_key, html_path in ICON_SITE_HTML.items():
        log.info(f"\n── Site: {site_key} ──")

        if not html_path.exists():
            log.warning(f"HTML file not found: {html_path} — skipping.")
            continue

        # Build data dict using the site-specific processor.
        # Each processor receives the full tables and filters internally.
        if site_key == "te-apiti":
            data = process_te_apiti(wp_all.copy(), pl_all.copy(), gis)
        elif site_key == "kia-wharite":
            data = process_kia_wharite()
        elif site_key == "manawatu-estuary":
            data = process_manawatu_estuary(wp_all.copy(), gis)
        elif site_key == "pukaha":
            data = process_pukaha(wp_all.copy(), pl_all.copy(), gis)
        elif site_key == "ruahine-kiwi":
            data = process_ruahine_kiwi(gis)
        else:
            log.warning(f"No processor defined for '{site_key}' — skipping.")
            continue

        # Write all-years summary CSV to Data/ (gitignored)
        csv_path = OUTPUT_DIR / f"{site_key}_summary_all_years.csv"
        all_years = data.get("allYears", [])
        if all_years:
            pd.DataFrame(all_years).to_csv(csv_path, index=False)
            log.info(f"  ✓ All-years CSV ({len(all_years)} rows) → {csv_path}")
        else:
            log.warning(f"  No all-years data to write for {site_key}")

        # Inject data directly into the HTML file
        inject_into_html(html_path, data)
        updated_html.append(html_path)

    if updated_html:
        git_commit_and_push(updated_html)

    log.info("\nIcon Sites Data Export — complete")
    log.info(f"Log: {log_path}")


if __name__ == "__main__":
    main()
