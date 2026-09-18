"""
Totara_Reserve_Data_Export.py
=============================
Pulls data for two Tōtara Reserve dashboards:

  1. river-management.html — LAWA swim quality + Horizons river level
       LAWA recreational water quality (E.coli + Cyanobacteria), site hrc-10013.
       Horizons EnviroData Hilltop proxy — Stage/Flow for Totara Reserve & Piripiri.

  2. predator-control.html — Predator control programme
       Animal Pest Control layer (AGOL) — trap inventory + catches by FY.
         PCOName IN ('Bio Totara', 'Bio Totara Wasp')
       PCO Monitoring Dataset (AGOL) — RTCI results.
         Label IN ('Oroua', 'Totara')
       PCO data requires the ArcGIS Pro Python environment (arcgis SDK, SSO auth).

Marker comments in river-management.html:
    /* SWIM_DATA_START */  /* SWIM_DATA_END */
    /* RIVER_DATA_START */ /* RIVER_DATA_END */

Marker comments in predator-control.html:
    /* PCO_DATA_START */   /* PCO_DATA_END */

Usage (ArcGIS Pro Python environment):
    python Totara_Reserve_Data_Export.py

LAWA data is cached in data/lawa/ and refreshed when older than CACHE_MAX_AGE_DAYS.
"""

import io
import json
import logging
import math
import re
import sys
import datetime
from pathlib import Path
import xml.etree.ElementTree as ET

import requests
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
try:
    import config
except ImportError:
    config = None  # PCO section will warn and skip; LAWA/river sections unaffected

# ── Paths ─────────────────────────────────────────────────────────────────────
HERE          = Path(__file__).parent
HTML_PATH     = HERE / "html" / "totara-reserve" / "river-management.html"
PCO_HTML_PATH = HERE / "html" / "totara-reserve" / "predator-control.html"
CACHE_DIR     = HERE / "data" / "lawa"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR = HERE / "logs" / "totara-reserve"
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_path = LOG_DIR / f"totara_reserve_{datetime.datetime.now():%Y%m%d_%H%M%S}.log"

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
LAWA_DOWNLOAD_PAGE = "https://www.lawa.org.nz/download-data"
LAWA_BASE_URL      = "https://www.lawa.org.nz"
LAWA_SITE_ID       = "hrc-10013"
CACHE_MAX_AGE_DAYS = 7
HISTORY_ROWS       = 12   # how many recent readings to include in history

# LAWA live API — supplements Excel with current-season status
LAWA_SWIMSITES_URL  = "https://www.lawa.org.nz/umbraco/api/mapservice/swimsites"
LAWA_SWIMSITE_PAGE  = "/explore-data/manawatu-whanganui-region/swimming/pohangina-river-at-totara-reserve/swimsite"
LAWA_LIVE_SITE_CODE = "HRC-10013"  # Code field in mapservice/swimsites response

# Horizons EnviroData Hilltop proxy — returns real-time telemetry; public access confirmed.
# Direct hilltopserver.horizons.govt.nz/data.hts returns "No data" for all sites publicly;
# the envirodata.horizons.govt.nz/api/hilltop proxy serves the same data without restrictions.
HILLTOP_URL   = "https://envirodata.horizons.govt.nz/api/hilltop"
TOTARA_SITE   = "Pohangina at Totara Reserve"
PIRIPIRI_SITE = "Pohangina at Piripiri"
STAGE_MEAS    = "Stage [Water Level]"  # mm — divide by 1000 for metres
FLOW_MEAS     = "Flow [Water Level]"   # L/s — divide by 1000 for m³/s; Totara Reserve has no rating curve

# ── PCO / Predator Control constants ──────────────────────────────────────────
TRAP_SERVICE_URL = getattr(config, "TRAP_SERVICE_URL", None) if config else None
TRAP_LAYER_ID    = getattr(config, "TRAP_LAYER_ID",    0)    if config else 0
INSP_TABLE_ID    = getattr(config, "INSP_TABLE_ID",    1)    if config else 1

TOTARA_PCO_WHERE = "PCOName IN ('Bio Totara', 'Bio Totara Wasp')"

CATCH_SPECIES = ["Cat", "Ferret", "Hedgehog", "Mouse", "Rabbit",
                 "Rat", "Stoat", "Possum", "Weasel"]

# Rodent TTI — local SharePoint-synced Excel. The path is per-machine and holds a
# user home directory, so it lives in the gitignored config.py rather than here.
_tti_path = getattr(config, "TOTARA_TTI_XLSX", None) if config else None
TTI_EXCEL_PATH = Path(_tti_path) if _tti_path else None

# Possum Bait Station layer (PC_Possum_Control_Layer_2025)
# Layer 1 = Bait Station features, Layer 2 = Inspection/fill records
POSSUM_SERVICE_URL   = "https://services1.arcgis.com/VuN78wcRdq1Oj69W/arcgis/rest/services/PC_Possum_Control_Layer_2025/FeatureServer"
POSSUM_BAIT_LAYER_ID = 1
POSSUM_INSP_LAYER_ID = 2

# Tōtara Reserve boundary — queried from HRC Icon Sites layer at runtime
ICON_SITES_URL      = "https://services1.arcgis.com/VuN78wcRdq1Oj69W/arcgis/rest/services/HRC_Icon_Sites_Projects/FeatureServer"
ICON_SITES_LAYER_ID = 0
TOTARA_SITE_NAME    = "Totara Reserve"

# Buffer distance around reserve polygon for possum bait station spatial filter
TOTARA_BUFFER_M = getattr(config, "TOTARA_BUFFER_M", 300) if config else 300

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "BioD-Hub/1.0 (HorizonsRC; internal dashboard)"})


# ── LAWA helpers ──────────────────────────────────────────────────────────────

def _find_lawa_excel_url() -> str:
    """Scrape the LAWA download page to find the current recreational WQ URL."""
    log.info("Fetching LAWA download page...")
    resp = SESSION.get(LAWA_DOWNLOAD_PAGE, timeout=30)
    resp.raise_for_status()
    # Match href containing "recreational-water-quality" and ending in .xlsx
    matches = re.findall(
        r'href=["\']([^"\']*recreational-water-quality[^"\']*\.xlsx)["\']',
        resp.text,
        re.IGNORECASE,
    )
    if not matches:
        raise RuntimeError(
            "Could not find recreational water quality download link on "
            f"{LAWA_DOWNLOAD_PAGE}. LAWA may have changed the page structure."
        )
    url = matches[0]
    if url.startswith("/"):
        url = LAWA_BASE_URL + url
    log.info(f"Found LAWA Excel URL: {url}")
    return url


def _download_lawa_excel(url: str) -> Path:
    """Download LAWA Excel to cache, return local path."""
    log.info(f"Downloading LAWA Excel ({url})...")
    resp = SESSION.get(url, timeout=120)
    resp.raise_for_status()
    filename = f"lawa_rwq_{datetime.datetime.now():%Y%m%d}.xlsx"
    path = CACHE_DIR / filename
    path.write_bytes(resp.content)
    log.info(f"Saved to {path} ({len(resp.content) / 1024:.0f} kB)")
    return path


def _get_lawa_dataframe() -> pd.DataFrame:
    """Return the monitoring data sheet as a DataFrame, using cache if fresh."""
    cached = sorted(CACHE_DIR.glob("lawa_rwq_*.xlsx"), reverse=True)
    if cached:
        age = datetime.datetime.now() - datetime.datetime.fromtimestamp(
            cached[0].stat().st_mtime
        )
        if age.days < CACHE_MAX_AGE_DAYS:
            log.info(f"Using cached file: {cached[0].name} ({age.days}d old)")
            xlsx_path = cached[0]
        else:
            log.info(f"Cache stale ({age.days}d) — refreshing...")
            xlsx_path = _download_lawa_excel(_find_lawa_excel_url())
    else:
        xlsx_path = _download_lawa_excel(_find_lawa_excel_url())

    xl = pd.ExcelFile(xlsx_path)
    # Sheet name contains year range and changes annually — detect dynamically
    sheet = next((s for s in xl.sheet_names if "MonitoringData" in s), None)
    if sheet is None:
        raise RuntimeError(
            f"No MonitoringData sheet found. Available sheets: {xl.sheet_names}"
        )
    log.info(f"Reading sheet '{sheet}'...")
    return xl.parse(sheet)


def _lawa_cssclass_to_icon(css_class: str) -> str:
    """Map LAWA mapservice CssClass (e.g. 'risk-high-weekly') to our icon key."""
    c = css_class.lower()
    if "risk-low" in c:
        return "green"
    if "risk-medium" in c or "risk-caution" in c:
        return "amber"
    if "risk-high" in c:
        return "red"
    return "unknown"


def _lawa_icon_to_description(icon: str) -> str:
    mapping = {"green": "Suitable for swimming", "amber": "Caution advised", "red": "Unsuitable for swimming"}
    return mapping.get(icon, "")


def fetch_lawa_live_status() -> dict | None:
    """
    Fetch current swim status from LAWA live API (mapservice/swimsites) and
    scrape the latest sample date from the swimsite page HTML.
    Returns a dict with keys: icon, description, date (YYYY-MM-DD), css_class.
    Returns None if either request fails.
    """
    browser_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.lawa.org.nz/",
        "Accept": "application/json",
    }
    try:
        log.info("Fetching LAWA live swim status...")
        resp = SESSION.get(LAWA_SWIMSITES_URL, headers=browser_headers, timeout=30)
        resp.raise_for_status()
        sites = resp.json()
        site = next(
            (s for s in sites if s.get("Code", "").upper() == LAWA_LIVE_SITE_CODE),
            None,
        )
        if not site:
            log.warning(f"  Site {LAWA_LIVE_SITE_CODE} not found in swimsites response.")
            return None
        css_class = site.get("CssClass", "")
        icon = _lawa_cssclass_to_icon(css_class)
        log.info(f"  Live CssClass: {css_class} -> icon: {icon}")
    except Exception as e:
        log.warning(f"  Could not fetch live swim status: {e}")
        return None

    # Scrape sample date from swimsite HTML (server-rendered)
    sample_date = None
    try:
        page_resp = SESSION.get(
            LAWA_BASE_URL + LAWA_SWIMSITE_PAGE,
            headers={**browser_headers, "Accept": "text/html"},
            timeout=20,
        )
        page_resp.raise_for_status()
        m = re.search(r"Last sampled (\d{1,2} \w+ \d{4})", page_resp.text)
        if m:
            raw = m.group(1)
            sample_date = datetime.datetime.strptime(raw, "%d %b %Y").strftime("%Y-%m-%d")
            log.info(f"  Scraped sample date: {raw} -> {sample_date}")
    except Exception as e:
        log.warning(f"  Could not scrape sample date from LAWA page: {e}")

    return {
        "icon":        icon,
        "description": _lawa_icon_to_description(icon),
        "date":        sample_date,
        "css_class":   css_class,
    }


def _apply_live_status(ecoli_data: dict, live: dict) -> None:
    """
    If the live LAWA status is more recent than the latest Excel reading,
    replace the 'latest' entry with the live status (value will be null
    because numeric readings aren't available from the live API).
    Mutates ecoli_data in place.
    """
    if not live or not live.get("date"):
        return
    live_dt = datetime.datetime.strptime(live["date"], "%Y-%m-%d")
    try:
        excel_dt = datetime.datetime.strptime(ecoli_data["latest"]["date"], "%Y-%m-%d")
    except (KeyError, ValueError):
        excel_dt = datetime.datetime.min

    if live_dt > excel_dt:
        log.info(
            f"  Live date {live['date']} is newer than Excel date "
            f"{ecoli_data['latest']['date']} — updating latest status."
        )
        ecoli_data["latest"] = {
            "date":        live["date"],
            "value":       None,
            "icon":        live["icon"],
            "description": live["description"],
        }
    elif live_dt == excel_dt and live["icon"] != ecoli_data["latest"].get("icon"):
        log.info(
            f"  Same date but different icon: live={live['icon']}, "
            f"excel={ecoli_data['latest'].get('icon')} — using live."
        )
        ecoli_data["latest"]["icon"]        = live["icon"]
        ecoli_data["latest"]["description"] = live["description"]
    else:
        log.info(
            f"  Excel data ({ecoli_data['latest']['date']}) is as recent as "
            f"live ({live['date']}) — no override needed."
        )


def extract_swim_data(df: pd.DataFrame) -> dict:
    """Extract E.coli and Cyanobacteria data for LAWA_SITE_ID."""
    site_df = df[df["LawaSiteID"].str.lower() == LAWA_SITE_ID].copy()
    if site_df.empty:
        raise RuntimeError(f"No rows found for site {LAWA_SITE_ID} in LAWA data.")
    site_df["DateCollected"] = pd.to_datetime(site_df["DateCollected"])
    site_df = site_df.sort_values("DateCollected", ascending=False)

    result = {}
    for indicator, key in [("E.coli", "ecoli"), ("Cyanobacteria", "cyanobacteria")]:
        ind_df = site_df[site_df["Indicator"] == indicator].reset_index(drop=True)
        if ind_df.empty:
            log.warning(f"No {indicator} data found for {LAWA_SITE_ID}")
            result[key] = None
            continue

        latest = ind_df.iloc[0]
        history = ind_df.head(HISTORY_ROWS)

        result[key] = {
            "latest": {
                "date":        latest["DateCollected"].strftime("%Y-%m-%d"),
                "value":       _safe_num(latest["Value"]),
                "icon":        str(latest.get("SwimIcon", "")).strip().lower(),
                "description": str(latest.get("SwimmingGuidelinesTestResultDescription", "")).strip(),
            },
            "history": [
                {
                    "date":  row["DateCollected"].strftime("%Y-%m-%d"),
                    "value": _safe_num(row["Value"]),
                    "icon":  str(row.get("SwimIcon", "")).strip().lower(),
                }
                for _, row in history.iterrows()
            ],
        }
        log.info(
            f"  {indicator}: latest {latest['DateCollected'].date()} "
            f"= {latest['Value']} ({latest.get('SwimIcon', '?')})"
        )

    # Supplement with live LAWA status (current season data not in Excel)
    live = fetch_lawa_live_status()
    if live and result.get("ecoli"):
        _apply_live_status(result["ecoli"], live)

    result["lastUpdated"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    return result


def _safe_num(v):
    try:
        f = float(v)
        return int(f) if f == int(f) else round(f, 1)
    except (TypeError, ValueError):
        return None


# ── Hilltop helpers ───────────────────────────────────────────────────────────

def _hilltop_latest(site: str, measurement: str) -> dict | None:
    """
    Fetch the most recent value via the Horizons EnviroData Hilltop proxy.
    EnviroData uses ISO 8601 timeInterval (e.g. 'P7D/now') instead of From/To.
    Response XML uses <E><T>timestamp</T><I1>value</I1></E> elements.
    Stage values are in mm; Flow values are in L/s.
    """
    params = {
        "service":      "Hilltop",
        "request":      "GetData",
        "site":         site,
        "measurement":  measurement,
        "timeInterval": "P7D/now",
        "interval":     "raw",
    }
    try:
        resp = SESSION.get(HILLTOP_URL, params=params, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        values = root.findall(".//E")
        if not values:
            log.warning(f"  No data: {site} / {measurement}")
            return None
        latest = values[-1]
        raw_val = latest.findtext("I1") or latest.findtext("I2")
        if raw_val is None:
            log.warning(f"  No value element in response: {site} / {measurement}")
            return None
        return {
            "value":     float(raw_val),
            "timestamp": latest.findtext("T") or "",
        }
    except ET.ParseError as e:
        log.error(f"  XML parse error ({site} / {measurement}): {e}")
        return None
    except Exception as e:
        log.error(f"  Hilltop error ({site} / {measurement}): {e}")
        return None


def extract_river_data() -> dict:
    """Fetch Stage and Flow for Tōtara Reserve and Piripiri via EnviroData proxy."""
    log.info("Fetching river data via EnviroData Hilltop proxy...")
    tr_stage = _hilltop_latest(TOTARA_SITE,   STAGE_MEAS)
    # Totara Reserve has no flow rating curve — skip flow fetch
    pp_stage = _hilltop_latest(PIRIPIRI_SITE, STAGE_MEAS)
    pp_flow  = _hilltop_latest(PIRIPIRI_SITE, FLOW_MEAS)

    def _stage_m(r):
        return round(r["value"] / 1000, 3) if r else None

    def _flow_m3s(r):
        return round(r["value"] / 1000, 3) if r else None

    def _site_block(stage_r, flow_r):
        return {
            "stage":     _stage_m(stage_r),
            "stageTime": stage_r["timestamp"] if stage_r else None,
            "flow":      _flow_m3s(flow_r),
            "flowTime":  flow_r["timestamp"]  if flow_r  else None,
        }

    river = {
        "totaraReserve": _site_block(tr_stage, None),
        "piripiri":      _site_block(pp_stage, pp_flow),
        "lastUpdated":   datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    for label, stage_r, flow_r in [
        ("Totara Reserve", tr_stage, None),
        ("Piripiri",       pp_stage, pp_flow),
    ]:
        sv = f"{_stage_m(stage_r)} m" if stage_r else "n/a"
        fv = f"{_flow_m3s(flow_r)} m3/s" if flow_r else "n/a"
        log.info(f"  {label}: stage={sv}  flow={fv}")

    return river


# ── PCO helpers ───────────────────────────────────────────────────────────────

def date_to_fy(dt) -> str | None:
    """Convert a datetime to a financial-year label, e.g. 2024-10-01 → '24-25'."""
    try:
        if pd.isna(dt):
            return None
    except (TypeError, ValueError):
        return None
    month, year = dt.month, dt.year
    if month >= 7:
        return f"{str(year)[2:]}-{str(year + 1)[2:]}"
    return f"{str(year - 1)[2:]}-{str(year)[2:]}"


def _fetch_agol_df(gis, service_url: str, layer_id: int, where: str = "1=1") -> pd.DataFrame:
    """Query an ArcGIS FeatureServer layer/table via the SDK, return as DataFrame."""
    from arcgis.features import FeatureLayer
    url   = f"{service_url.rstrip('/')}/{layer_id}"
    layer = FeatureLayer(url, gis=gis)
    name  = getattr(layer.properties, "name", url.split("/")[-1])
    log.info(f"  Querying {name} (layer {layer_id}) where: {where[:80]}...")
    fset  = layer.query(where=where, out_fields="*", return_geometry=False)
    df    = fset.sdf
    log.info(f"    → {len(df):,} records")
    return df


# ── PCO data extraction ────────────────────────────────────────────────────────

def extract_pco_data() -> dict | None:
    """
    Fetch predator control data for Tōtara Reserve from AGOL.

    Trap inventory + catch records: Animal Pest Control layer
      PCOName IN ('Bio Totara', 'Bio Totara Wasp')
      Bio Totara        → mustelid/rodent kill traps
      Bio Totara Wasp   → Vespex wasp bait stations

    RTCI monitoring results: PCO Monitoring Dataset
      Label IN ('Oroua', 'Totara')

    Requires ArcGIS Pro Python environment (arcgis SDK) for authenticated
    AGOL access.  Returns None on failure so the rest of the script continues.
    """
    log.info("Processing PCO / Predator Control data...")

    if not TRAP_SERVICE_URL:
        log.warning("  TRAP_SERVICE_URL not set — skipping PCO data.")
        return None

    try:
        from arcgis.gis import GIS
        gis = GIS("pro")
        log.info(f"  Connected to AGOL as: {gis.properties.user.username}")
    except Exception as exc:
        log.error(f"  Cannot connect to AGOL — PCO data skipped: {exc}")
        return None

    bio_totara = {"total": 0, "byType": {"labels": [], "data": []}}
    vespex     = {"total": 0, "byType": {"labels": [], "data": []}}
    catches_by_fy: dict = {"labels": [], "species": [], "data": {}}

    # ── Trap inventory + catch records ────────────────────────────────────────
    try:
        traps = _fetch_agol_df(gis, TRAP_SERVICE_URL, TRAP_LAYER_ID,
                               where=TOTARA_PCO_WHERE)
        log.info(f"  Trap columns: {sorted(traps.columns.tolist())}")

        if not traps.empty and "PCOName" in traps.columns:
            bio_df  = traps[traps["PCOName"] == "Bio Totara"]
            wasp_df = traps[traps["PCOName"] == "Bio Totara Wasp"]

            bio_totara["total"] = int(len(bio_df))
            if "TrapType" in traps.columns and not bio_df.empty:
                tc = bio_df["TrapType"].value_counts()
                bio_totara["byType"] = {
                    "labels": list(tc.index),
                    "data":   [int(v) for v in tc.values],
                }

            vespex["total"] = int(len(wasp_df))
            if "TrapType" in traps.columns and not wasp_df.empty:
                tc = wasp_df["TrapType"].value_counts()
                vespex["byType"] = {
                    "labels": list(tc.index),
                    "data":   [int(v) for v in tc.values],
                }

            log.info(f"  Bio Totara traps: {bio_totara['total']}, "
                     f"Vespex stations: {vespex['total']}")
            log.info(f"  Bio Totara types: {bio_totara['byType']}")
            log.info(f"  Vespex types: {vespex['byType']}")

            # Catch records — pull full inspection table and filter by trap GlobalID
            trap_ids = (
                set(traps["GlobalID"].dropna().astype(str)
                    .str.strip("{}").str.lower().tolist())
                if "GlobalID" in traps.columns else set()
            )

            try:
                insp_all = _fetch_agol_df(gis, TRAP_SERVICE_URL, INSP_TABLE_ID,
                                          where="1=1")
            except Exception as exc:
                log.warning(f"  Inspection table query failed: {exc}")
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
                    norm = insp_all[join_col].astype(str).str.strip("{}").str.lower()
                    insp = insp_all[norm.isin(trap_ids)].copy()
                else:
                    insp = pd.DataFrame()
                log.info(f"  Inspection records for Totara traps: {len(insp):,}")

                if not insp.empty and "created_date" in insp.columns \
                        and "SpeciesCaught" in insp.columns:
                    insp["_dt"] = pd.to_datetime(insp["created_date"],
                                                 unit="ms", errors="coerce")
                    insp["_fy"] = insp["_dt"].apply(date_to_fy)

                    # Log every species present so the user can verify what's captured
                    all_sp = insp["SpeciesCaught"].dropna().value_counts()
                    log.info(f"  All species in inspection records: {dict(all_sp)}")

                    # Filter to actual catches
                    exclude = {"", "nothing caught", "nil", "none", "no catch"}
                    caught = insp[
                        insp["SpeciesCaught"].notna() &
                        ~insp["SpeciesCaught"].str.strip().str.lower().isin(exclude)
                    ].copy()
                    log.info(f"  Records with a catch: {len(caught):,}")

                    if not caught.empty:
                        # Use CATCH_SPECIES list where species are present; fall back
                        # to top-8 by count for anything not in the list.
                        present = [s for s in CATCH_SPECIES
                                   if s in caught["SpeciesCaught"].values]
                        extra   = [s for s in caught["SpeciesCaught"].unique()
                                   if s not in CATCH_SPECIES]
                        if extra:
                            log.info(f"  Species outside CATCH_SPECIES list: {extra}")
                        display_sp = present if present else list(
                            caught["SpeciesCaught"].value_counts().head(8).index
                        )

                        pivot = (
                            caught[caught["SpeciesCaught"].isin(display_sp)]
                            .groupby(["_fy", "SpeciesCaught"])
                            .size()
                            .unstack(fill_value=0)
                        )
                        fy_order = sorted(
                            [fy for fy in pivot.index if fy],
                            key=lambda s: s.split("-")[0]
                        )
                        pivot = pivot.reindex(fy_order)

                        catches_by_fy = {
                            "labels":  fy_order,
                            "species": display_sp,
                            "data": {
                                sp: [
                                    int(pivot.at[fy, sp])
                                    if sp in pivot.columns else 0
                                    for fy in fy_order
                                ]
                                for sp in display_sp
                            },
                        }
                        log.info(f"  Catches by FY: "
                                 f"{list(zip(fy_order, [sum(catches_by_fy['data'][s][i] for s in display_sp) for i in range(len(fy_order))]))}")

    except Exception as exc:
        log.warning(f"  Trap/inspection query failed: {exc}")

    # ── Possum bait stations ──────────────────────────────────────────────────
    possum_bait: dict = {
        "withinReserve": None,
        "withinBuffer":  None,
        "bufferM":       TOTARA_BUFFER_M,
        "fills":         {"labels": [], "fillCounts": {}},
    }
    try:
        from arcgis.features import FeatureLayer as _FL
        from arcgis.geometry.filters import intersects as _geo_intersects

        # Fetch the actual reserve boundary polygon from the Icon Sites layer
        log.info("  Fetching Totara Reserve boundary from HRC Icon Sites layer...")
        sites_layer = _FL(f"{ICON_SITES_URL}/{ICON_SITES_LAYER_ID}", gis=gis)
        boundary_fset = sites_layer.query(
            where=f"SiteName = '{TOTARA_SITE_NAME}'",
            out_fields="SiteName",
            return_geometry=True,
            out_sr=4326,
        )
        if not boundary_fset.features:
            log.warning(f"  '{TOTARA_SITE_NAME}' not found in Icon Sites layer — spatial filter skipped")
            reserve_geom = None
        else:
            reserve_geom = boundary_fset.features[0].geometry
            log.info(f"  Reserve boundary fetched: {boundary_fset.features[0].attributes.get('SiteName', '?')}")

        log.info("  Querying Possum Bait Station layer (PC_Possum_Control_Layer_2025)...")
        bait_layer = _FL(f"{POSSUM_SERVICE_URL}/{POSSUM_BAIT_LAYER_ID}", gis=gis)

        if reserve_geom:
            # Build a buffered bounding box from the polygon extent.
            # The reserve polygon is used directly for the "within reserve" query (accurate).
            # The buffer zone uses the extent expanded by TOTARA_BUFFER_M in degrees (avoids
            # dependency on the AGOL geometry service, which may not be configured for Pro SSO).
            ext     = reserve_geom.extent
            lat_mid = (ext["ymin"] + ext["ymax"]) / 2
            buf_lat = TOTARA_BUFFER_M / 111_111
            buf_lon = TOTARA_BUFFER_M / (111_111 * math.cos(math.radians(lat_mid)))
            buf_env = {
                "xmin": ext["xmin"] - buf_lon,
                "ymin": ext["ymin"] - buf_lat,
                "xmax": ext["xmax"] + buf_lon,
                "ymax": ext["ymax"] + buf_lat,
                "spatialReference": {"wkid": 4326},
            }

            fset_res = bait_layer.query(
                geometry_filter=_geo_intersects(reserve_geom, sr=4326),
                out_fields="GlobalID",
                return_geometry=False,
            )
            fset_buf = bait_layer.query(
                geometry_filter=_geo_intersects(buf_env, sr=4326),
                out_fields="*",
                return_geometry=False,
            )
            n_res = len(fset_res.features)
            n_buf = len(fset_buf.features)
            possum_bait["withinReserve"] = n_res
            possum_bait["withinBuffer"]  = n_buf - n_res
            log.info(f"  Possum bait: {n_res} in reserve, "
                     f"{n_buf - n_res} in {TOTARA_BUFFER_M}m buffer zone")
            possum_stations = fset_buf.sdf
        else:
            log.warning("  No reserve geometry — querying all possum bait stations (no spatial filter)")
            fset_all = bait_layer.query(where="1=1", out_fields="*", return_geometry=False)
            possum_stations = fset_all.sdf
            log.info(f"  Possum bait stations (no spatial filter): {len(possum_stations)}")

        log.info(f"  Possum station columns: {sorted(possum_stations.columns.tolist())}")

        if not possum_stations.empty:
            station_ids = (
                set(possum_stations["GlobalID"].dropna().astype(str)
                    .str.strip("{}").str.lower().tolist())
                if "GlobalID" in possum_stations.columns else set()
            )

            # Query inspection/fill records (layer 2)
            try:
                possum_insp = _fetch_agol_df(
                    gis, POSSUM_SERVICE_URL, POSSUM_INSP_LAYER_ID, where="1=1"
                )
                log.info(f"  Possum inspection columns: {sorted(possum_insp.columns.tolist())}")

                if not possum_insp.empty and station_ids:
                    join_col = None
                    for cand in ("TrapParentID", "ParentGlobalID", "ParentID"):
                        if cand in possum_insp.columns:
                            join_col = cand
                            break
                    if join_col is None:
                        pid_cols = [c for c in possum_insp.columns if "parent" in c.lower()]
                        if pid_cols:
                            join_col = pid_cols[0]
                    log.info(f"  Possum inspection join column: {join_col}")

                    if join_col:
                        norm  = possum_insp[join_col].astype(str).str.strip("{}").str.lower()
                        insp_f = possum_insp[norm.isin(station_ids)].copy()
                        log.info(f"  Possum inspection records matched: {len(insp_f):,}")

                        date_col = next(
                            (c for c in insp_f.columns
                             if "date" in c.lower() or "created" in c.lower()), None
                        )
                        fill_col = next(
                            (c for c in insp_f.columns
                             if any(kw in c.lower() for kw in ("fill", "visit", "round"))),
                            None,
                        )
                        log.info(f"  Date col: {date_col}  Fill col: {fill_col}")

                        if date_col and fill_col and not insp_f.empty:
                            insp_f["_dt"] = pd.to_datetime(
                                insp_f[date_col], unit="ms", errors="coerce"
                            )
                            insp_f["_fy"] = insp_f["_dt"].apply(date_to_fy)
                            fill_grp = (
                                insp_f.groupby(["_fy", fill_col])
                                .size()
                                .unstack(fill_value=0)
                            )
                            fy_order = sorted(
                                [fy for fy in fill_grp.index if fy],
                                key=lambda s: s.split("-")[0],
                            )
                            fill_grp  = fill_grp.reindex(fy_order)
                            fill_keys = sorted(fill_grp.columns.tolist(), key=str)
                            possum_bait["fills"] = {
                                "labels": fy_order,
                                "fillCounts": {
                                    str(k): [
                                        int(fill_grp.at[fy, k])
                                        if k in fill_grp.columns else 0
                                        for fy in fy_order
                                    ]
                                    for k in fill_keys
                                },
                            }
                            log.info(f"  Possum fills: {fy_order}, keys: {fill_keys}")

            except Exception as exc:
                log.warning(f"  Possum inspection query failed: {exc}")

    except Exception as exc:
        log.warning(f"  Possum bait station query failed: {exc}")

    return {
        "generated":   datetime.datetime.now().isoformat(),
        "bioTotara":   bio_totara,
        "vespex":      vespex,
        "catchesByFy": catches_by_fy,
        "possumBait":  possum_bait,
    }



# ── TTI (Rodent Tracking Tunnel Index) ────────────────────────────────────────

def extract_tti_data() -> dict | None:
    """
    Read rodent Tracking Tunnel Index data from the local SharePoint-synced Excel.
    Columns used: Location, Date, Rat TTI, Mouse TTI (values 0–1 scale).
    Filters to Tōtara Reserve rows; averages across monitoring sites per date.
    Returns None if the file is missing, locked, or unreadable.
    """
    if TTI_EXCEL_PATH is None:
        log.warning("TOTARA_TTI_XLSX not set in config.py — skipping rodent TTI.")
        return None
    if not TTI_EXCEL_PATH.exists():
        log.warning(f"TTI Excel not found: {TTI_EXCEL_PATH}")
        return None
    try:
        df = pd.read_excel(TTI_EXCEL_PATH, usecols=[0, 1, 2, 3])
        df.columns = ["Location", "Date", "Rat_TTI", "Mouse_TTI"]
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Location", "Date"])

        # Keep Totara Reserve rows; exclude Kahikatea
        totara = df[
            df["Location"].str.contains("Reserve", na=False) &
            ~df["Location"].str.contains("Kahikatea", na=False)
        ].copy()

        if totara.empty:
            log.warning("  No Totara Reserve TTI records found in Excel.")
            return None

        # Average across monitoring sites per date (pre-2020 has multiple lines per date)
        grouped = (
            totara.groupby("Date")
            .agg(rat=("Rat_TTI", "mean"), mouse=("Mouse_TTI", "mean"))
            .reset_index()
            .sort_values("Date")
        )

        def _pct(v):
            try:
                return round(float(v) * 100, 1) if not pd.isna(v) else None
            except (TypeError, ValueError):
                return None

        latest = grouped.iloc[-1]
        result = {
            "latest": {
                "date":  latest["Date"].strftime("%Y-%m-%d"),
                "rat":   _pct(latest["rat"]),
                "mouse": _pct(latest["mouse"]),
            },
            "history": {
                "dates": [d.strftime("%Y-%m-%d") for d in grouped["Date"]],
                "rat":   [_pct(v) for v in grouped["rat"]],
                "mouse": [_pct(v) for v in grouped["mouse"]],
            },
        }
        log.info(f"  TTI latest ({latest['Date'].date()}): "
                 f"Rat={result['latest']['rat']}%  Mouse={result['latest']['mouse']}%")
        log.info(f"  TTI history: {len(grouped)} dates, "
                 f"{grouped['Date'].dt.year.min()}–{grouped['Date'].dt.year.max()}")
        return result

    except PermissionError:
        log.warning(f"  TTI Excel is open in Excel — close it and rerun: {TTI_EXCEL_PATH.name}")
        return None
    except Exception as exc:
        log.error(f"  TTI Excel read failed: {exc}")
        return None


# ── HTML injection ─────────────────────────────────────────────────────────────

def _replace_block(html: str, start_marker: str, end_marker: str, new_content: str) -> str:
    pattern = (
        rf"(/\* {re.escape(start_marker)} \*/\n)"
        rf".*?"
        rf"(\n\s*/\* {re.escape(end_marker)} \*/)"
    )
    replacement = rf"\g<1>{new_content}\g<2>"
    result, n = re.subn(pattern, replacement, html, flags=re.DOTALL)
    if n == 0:
        log.warning(f"Marker /* {start_marker} */ not found in {HTML_PATH.name} — skipping")
    return result


def inject_into_html(swim: dict, river: dict) -> None:
    if not HTML_PATH.exists():
        log.error(f"{HTML_PATH} does not exist — skipping injection.")
        return
    html = HTML_PATH.read_text(encoding="utf-8")
    html = _replace_block(
        html, "SWIM_DATA_START", "SWIM_DATA_END",
        f"const SWIM_DATA = {json.dumps(swim, indent=2, ensure_ascii=False)};"
    )
    html = _replace_block(
        html, "RIVER_DATA_START", "RIVER_DATA_END",
        f"const RIVER_DATA = {json.dumps(river, indent=2, ensure_ascii=False)};"
    )
    HTML_PATH.write_text(html, encoding="utf-8")
    log.info(f"Updated {HTML_PATH}")


def inject_into_pco_html(pco: dict) -> None:
    if not PCO_HTML_PATH.exists():
        log.warning(f"{PCO_HTML_PATH} does not exist — skipping PCO injection.")
        return
    html = PCO_HTML_PATH.read_text(encoding="utf-8")
    html = _replace_block(
        html, "PCO_DATA_START", "PCO_DATA_END",
        f"const PCO_DATA = {json.dumps(pco, indent=2, ensure_ascii=False)};"
    )
    PCO_HTML_PATH.write_text(html, encoding="utf-8")
    log.info(f"Updated {PCO_HTML_PATH}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Totara Reserve Data Export ===")

    log.info("--- LAWA Recreational Water Quality ---")
    df   = _get_lawa_dataframe()
    swim = extract_swim_data(df)

    log.info("--- Hilltop River Level / Flow ---")
    river = extract_river_data()

    log.info("--- Injecting into river-management.html ---")
    inject_into_html(swim, river)

    log.info("--- PCO / Predator Control (requires ArcGIS Pro env) ---")
    pco = extract_pco_data()
    if pco:
        log.info("--- Rodent Tracking Tunnel Index (TTI) ---")
        tti = extract_tti_data()
        if tti:
            pco["tti"] = tti
        log.info("--- Injecting into predator-control.html ---")
        inject_into_pco_html(pco)
    else:
        log.warning("PCO data unavailable — predator-control.html not updated.")

    log.info("=== Done ===")


if __name__ == "__main__":
    main()
