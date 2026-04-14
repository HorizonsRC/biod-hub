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
import datetime
from pathlib import Path

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

if not CONTRACTOR_ITEM_ID or not TRAP_SERVICE_URL:
    sys.exit(
        "ERROR: CONTRACTOR_ITEM_ID and TRAP_SERVICE_URL must be set in config.py. "
        "See config.example.py for the required keys."
    )

# PCO zone names that make up the Te Apiti / Manawatu Gorge programme
TE_APITI_PCO_WHERE = (
    "PCOName IN ("
    "'Bio Te Apiti', 'Bio Te Apiti Buffer', "
    "'Bio Te Apiti Buffer North', 'Bio Te Apiti Buffer South', "
    "'Bio Te Apiti South'"
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
    "te-apiti": HTML_DIR / "te-apiti.html",
    # Add further sites here as they are developed:
    # "bushy-park": HTML_DIR / "bushy-park.html",
}

# Output directory for per-site summary CSVs (gitignored via Outputs/)
OUTPUT_DIR = Path(getattr(config, "ICON_SITES_OUTPUT_DIR", REPO_ROOT / "Outputs" / "icon-sites"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
        "allYears": all_years_rows,
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
