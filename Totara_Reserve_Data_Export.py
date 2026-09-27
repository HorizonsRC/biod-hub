"""
Totara_Reserve_Data_Export.py
=============================
Pulls data for two Tōtara Reserve dashboards:

  1. river-management.html — LAWA swim quality + Horizons river level
       LAWA recreational water quality (E.coli + Cyanobacteria), site hrc-10013.
       Horizons EnviroData Hilltop proxy — Stage/Flow for Totara Reserve & Piripiri.

  2. predator-control.html — Predator control programme
       Features are selected by a live intersect against the Tōtara Reserve
       polygon (HRC Icon Sites layer, SiteName = 'Totara Reserve').
       Animal Pest Control layer (AGOL) — trap inventory + catches by FY.
       PC_Possum_Control_Layer_2025 (AGOL) — possum bait stations + fills by FY.
       Rodent tracking indices spreadsheet (config.TOTARA_TTI_XLSX) — TTI.
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
# The pest plant page is three embeds sharing one data block: the header strip,
# the charts panel beside the map, and the legend floated over the map.
PEST_PLANT_HTML_PATHS = [
    HERE / "html" / "totara-reserve" / name
    for name in ("pest-plant-header.html", "pest-plant-control.html", "pest-plant-legend.html")
]
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

CATCH_SPECIES = ["Cat", "Ferret", "Hedgehog", "Mouse", "Rabbit",
                 "Rat", "Stoat", "Possum", "Weasel"]

# Rodent TTI — local SharePoint-synced Excel. The path is per-machine and holds a
# user home directory, so it lives in the gitignored config.py rather than here.
_tti_path = getattr(config, "TOTARA_TTI_XLSX", None) if config else None
TTI_EXCEL_PATH = Path(_tti_path) if _tti_path else None

# Possum Bait Station layer (PC_Possum_Control_Layer_2025)
# Layer 0 = Bait Station features, layer 1 = Points of Interest, table 2 = Inspection
POSSUM_SERVICE_URL   = "https://services1.arcgis.com/VuN78wcRdq1Oj69W/arcgis/rest/services/PC_Possum_Control_Layer_2025/FeatureServer"
POSSUM_BAIT_LAYER_ID = 0
POSSUM_INSP_LAYER_ID = 2

# Tōtara Reserve boundary — queried from HRC Icon Sites layer at runtime. Traps and
# possum bait stations are selected by a live intersect against this polygon.
ICON_SITES_URL      = "https://services1.arcgis.com/VuN78wcRdq1Oj69W/arcgis/rest/services/HRC_Icon_Sites_Projects/FeatureServer"
ICON_SITES_LAYER_ID = 0
TOTARA_SITE_NAME    = "Totara Reserve"

# Traps within this distance of the reserve boundary count as reserve traps — the
# trap lines run along and just outside the edge. Possum bait stations use none.
TRAP_BUFFER_M = 200

# Pest plant contractor data — Tōtara_Reserve_Contractor_Data, a separate service
# from the icon sites' BioD contractor item. Layer 1 = waypoints (one row per weed
# location), layer 2 = polylines (GPS tracks walked). Both already hold only this
# site (SiteID 'Man240'), so no spatial or site filter is needed.
PEST_PLANT_ITEM_ID      = "f865a454aaa645a6a2f88a07add7f12c"
PEST_PLANT_POINTS_LAYER = 1
PEST_PLANT_TRACKS_LAYER = 2
# Tōtara Reserve web map. Species colours, the size key and the layer titles are
# read from its symbology so the dashboard always matches what the map draws.
TOTARA_WEBMAP_ID        = "e3d60ce2a731408f9e25126fc4e2ef7d"

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


def _fetch_reserve_boundary(gis):
    """
    Fetch the Tōtara Reserve polygon from the HRC Icon Sites layer.

    Kept in the layer's own spatial reference, so the intersect against the pest
    layers does not depend on a reprojection. Returns None if the site is missing.
    """
    from arcgis.features import FeatureLayer
    log.info("  Fetching Tōtara Reserve boundary from HRC Icon Sites layer...")
    layer = FeatureLayer(f"{ICON_SITES_URL}/{ICON_SITES_LAYER_ID}", gis=gis)
    fset  = layer.query(
        where=f"SiteName = '{TOTARA_SITE_NAME}'",
        out_fields="SiteName",
        return_geometry=True,
    )
    if not fset.features:
        log.warning(f"  '{TOTARA_SITE_NAME}' not found in Icon Sites layer")
        return None
    geom = fset.features[0].geometry
    log.info(f"  Reserve boundary fetched (wkid "
             f"{geom.get('spatialReference', {}).get('wkid', '?')})")
    return geom


def _fetch_in_reserve(gis, service_url: str, layer_id: int, reserve_geom,
                      buffer_m: float = 0) -> pd.DataFrame:
    """
    Query a FeatureServer layer for every feature intersecting the reserve polygon,
    or within buffer_m metres of it.

    Sent as a POST: the polygon is ~1,400 vertices (~42 KB of JSON), too long for
    the GET URL FeatureLayer.query() builds, which AGOL rejects with 502s.
    Pages through results in case the layer's maxRecordCount is exceeded.
    """
    from arcgis.features import FeatureLayer
    url   = f"{service_url.rstrip('/')}/{layer_id}"
    layer = FeatureLayer(url, gis=gis)
    name  = getattr(layer.properties, "name", url.split("/")[-1])
    sr    = reserve_geom.get("spatialReference", {}).get("wkid")
    log.info(f"  Querying {name} (layer {layer_id}) intersecting the reserve"
             f"{f' + {buffer_m:g} m' if buffer_m else ''}...")

    params = {
        "f":              "json",
        "where":          "1=1",
        "geometry":       json.dumps(reserve_geom),
        "geometryType":   "esriGeometryPolygon",
        "inSR":           sr,
        "spatialRel":     "esriSpatialRelIntersects",
        "outFields":      "*",
        "returnGeometry": "false",
    }
    if buffer_m:
        params.update({"distance": buffer_m, "units": "esriSRUnit_Meter"})

    rows, offset = [], 0
    while True:
        resp = gis._con.post(f"{url}/query", {**params, "resultOffset": offset})
        if "error" in resp:
            raise RuntimeError(f"{name} query error: {resp['error']}")
        feats = resp.get("features", [])
        rows.extend(f["attributes"] for f in feats)
        if not resp.get("exceededTransferLimit") or not feats:
            break
        offset += len(feats)

    df = pd.DataFrame(rows)
    log.info(f"    → {len(df):,} features in reserve")
    if "GlobalID" in df.columns:
        log.debug(f"    {name} GlobalIDs in reserve: "
                  f"{sorted(df['GlobalID'].dropna().astype(str).tolist())}")
    return df


def _find_parent_col(df: pd.DataFrame) -> str | None:
    """Find the column in a related table that holds the parent feature's GlobalID."""
    for cand in ("TrapParentID", "ParentGlobalID", "ParentID"):
        if cand in df.columns:
            return cand
    pid_cols = [c for c in df.columns if "parent" in c.lower()]
    return pid_cols[0] if pid_cols else None


def _norm_ids(series: pd.Series) -> pd.Series:
    """Normalise GlobalIDs for joining: no braces, lower case."""
    return series.dropna().astype(str).str.strip("{}").str.lower()


# ── PCO data extraction ────────────────────────────────────────────────────────

def extract_pco_data() -> dict | None:
    """
    Fetch predator control data for Tōtara Reserve from AGOL.

    Selection is a live spatial intersect against the Tōtara Reserve polygon
    (HRC Icon Sites, SiteName = 'Totara Reserve'), repeated every run so traps
    and stations added or removed later are picked up.

    Traps + catches: Animal Pest Control layer (layer 0) + inspection table (1).
      Every trap in the reserve or within TRAP_BUFFER_M of it, whoever the PCO. TrapType containing "Vespex"
      → wasp bait stations; everything else → animal traps.

    Possum bait stations: PC_Possum_Control_Layer_2025 (layer 1) + fills (2).

    Requires ArcGIS Pro Python environment (arcgis SDK) for authenticated
    AGOL access. Returns None on failure, or when either intersect comes back
    empty, so the page keeps its last good data rather than showing zeros.
    """
    log.info("Processing PCO / Predator Control data...")

    if not TRAP_SERVICE_URL:
        log.warning("  TRAP_SERVICE_URL not set — skipping PCO data.")
        return None

    try:
        from arcgis.gis import GIS
        gis = GIS("pro")
        log.info(f"  Connected to AGOL as: {gis.properties.user.username}")
    except Exception:
        log.exception("  Cannot connect to AGOL — PCO data skipped")
        return None

    try:
        reserve_geom = _fetch_reserve_boundary(gis)
    except Exception:
        log.exception("  Reserve boundary query failed — PCO data skipped")
        return None
    if reserve_geom is None:
        return None

    bio_totara = {"total": 0, "byType": {"labels": [], "data": []}}
    vespex     = {"total": 0, "byType": {"labels": [], "data": []}}
    catches_by_fy: dict = {"labels": [], "species": [], "data": {}}

    # ── Trap inventory + catch records ────────────────────────────────────────
    try:
        traps = _fetch_in_reserve(gis, TRAP_SERVICE_URL, TRAP_LAYER_ID, reserve_geom,
                                  buffer_m=TRAP_BUFFER_M)
    except Exception:
        log.exception("  Trap intersect query failed — PCO data skipped")
        return None
    if traps.empty:
        log.warning("  No traps found in the reserve — treating as a failed query, "
                    "not zero. predator-control.html left as it was.")
        return None

    try:
        log.info(f"  Trap columns: {sorted(traps.columns.tolist())}")
        if "PCOName" in traps.columns:
            log.info(f"  PCOName counts: {dict(traps['PCOName'].value_counts(dropna=False))}")

        if "TrapType" in traps.columns:
            traps["TrapType"] = traps["TrapType"].str.strip()
            log.info(f"  TrapType counts: {dict(traps['TrapType'].value_counts(dropna=False))}")
            is_wasp = traps["TrapType"].astype(str).str.contains("vespex", case=False)
        else:
            log.warning("  No TrapType column — cannot split Vespex from traps")
            is_wasp = pd.Series(False, index=traps.index)
        bio_df  = traps[~is_wasp]
        wasp_df = traps[is_wasp]

        for out, df in ((bio_totara, bio_df), (vespex, wasp_df)):
            out["total"] = int(len(df))
            if "TrapType" in df.columns and not df.empty:
                tc = df["TrapType"].value_counts()
                out["byType"] = {
                    "labels": list(tc.index),
                    "data":   [int(v) for v in tc.values],
                }

        log.info(f"  Animal traps: {bio_totara['total']}, "
                 f"Vespex stations: {vespex['total']}")
        log.info(f"  Animal trap types: {bio_totara['byType']}")
        log.info(f"  Vespex types: {vespex['byType']}")

        # Catch records — pull full inspection table and filter by trap GlobalID
        trap_ids = (set(_norm_ids(traps["GlobalID"]))
                    if "GlobalID" in traps.columns else set())

        try:
            insp_all = _fetch_agol_df(gis, TRAP_SERVICE_URL, INSP_TABLE_ID,
                                      where="1=1")
        except Exception:
            log.exception("  Inspection table query failed")
            insp_all = pd.DataFrame()

        if not insp_all.empty:
            log.info(f"  Inspection columns: {sorted(insp_all.columns.tolist())}")

            join_col = _find_parent_col(insp_all)
            log.info(f"  Inspection join column: {join_col}")

            if join_col and trap_ids:
                norm = insp_all[join_col].astype(str).str.strip("{}").str.lower()
                insp = insp_all[norm.isin(trap_ids)].copy()
            else:
                insp = pd.DataFrame()
            log.info(f"  Inspection records for reserve traps: {len(insp):,}")

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

    except Exception:
        log.exception("  Trap/inspection processing failed")

    # ── Possum bait stations ──────────────────────────────────────────────────
    possum_bait: dict = {
        "withinReserve": None,
        "fills":         {"labels": [], "fillCounts": {}},
    }
    try:
        stations = _fetch_in_reserve(gis, POSSUM_SERVICE_URL, POSSUM_BAIT_LAYER_ID,
                                     reserve_geom)
    except Exception:
        log.exception("  Possum bait station intersect query failed — PCO data skipped")
        return None
    if stations.empty:
        log.warning("  No possum bait stations found in the reserve — treating as a "
                    "failed query, not zero. predator-control.html left as it was.")
        return None

    possum_bait["withinReserve"] = int(len(stations))
    log.info(f"  Possum bait stations in reserve: {possum_bait['withinReserve']}")
    log.info(f"  Possum station columns: {sorted(stations.columns.tolist())}")

    try:
        station_ids = (set(_norm_ids(stations["GlobalID"]))
                       if "GlobalID" in stations.columns else set())

        possum_insp = _fetch_agol_df(
            gis, POSSUM_SERVICE_URL, POSSUM_INSP_LAYER_ID, where="1=1"
        )
        log.info(f"  Possum inspection columns: {sorted(possum_insp.columns.tolist())}")

        if not possum_insp.empty and station_ids:
            join_col = _find_parent_col(possum_insp)
            log.info(f"  Possum inspection join column: {join_col}")

            if join_col:
                norm   = possum_insp[join_col].astype(str).str.strip("{}").str.lower()
                insp_f = possum_insp[norm.isin(station_ids)].copy()
                log.info(f"  Possum inspection records matched: {len(insp_f):,}")

                # Each inspection record is one fill visit; split by toxin
                date_col = "created_date"
                fill_col = "Toxin"
                if fill_col in insp_f.columns:
                    log.info(f"  Toxin counts: {dict(insp_f[fill_col].value_counts(dropna=False))}")
                    insp_f[fill_col] = insp_f[fill_col].fillna("Not recorded")

                if date_col in insp_f.columns and fill_col in insp_f.columns                         and not insp_f.empty:
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

    except Exception:
        log.exception("  Possum inspection query failed")

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


# ── Pest plants ────────────────────────────────────────────────────────────────

def previous_financial_year(today=None) -> tuple[str, str]:
    """The FY most recently finished, as ('25-26', '2025-26').

    Copied from Icon_Sites_Data_Export.py rather than imported — importing that
    module sets up its own log file and exits when its config keys are missing.
    The page reports on a completed year: the current FY's records are not all
    in until it closes.
    """
    today = today or datetime.datetime.now()
    start = today.year - 1 if today.month >= 7 else today.year - 2
    return f"{str(start)[2:]}-{str(start + 1)[2:]}", f"{start}-{str(start + 1)[2:]}"


def latest_fy_with_records(df, fy_col: str, wanted: str, what: str = "records") -> str:
    """`wanted` if it has rows in `df`, else the newest FY that does.

    Copied from Icon_Sites_Data_Export.py. Contractor data arrives well after a
    year closes; publishing the empty year would zero every figure, so the page
    label follows the data and rolls forward on its own once the records land.
    """
    if fy_col not in df.columns or not df[df[fy_col] == wanted].empty:
        return wanted
    available = sorted(df[fy_col].dropna().unique())
    if not available:
        return wanted
    fallback = available[-1]
    log.warning(
        f"  No {what} for FY {wanted} yet -- showing FY {fallback} instead. "
        f"The page label rolls to {wanted} once its records land."
    )
    return fallback


def _rgba_to_hex(rgba) -> str | None:
    if not rgba or len(rgba) < 3:
        return None
    return "#{:02x}{:02x}{:02x}".format(*[int(c) for c in rgba[:3]])


def _fetch_pest_plant_symbology(gis) -> dict:
    """Species colours, size key and layer titles from the Tōtara web map.

    Layers are matched on their service URL, not their title, so renaming them in
    the map does not break this. Returns {} if the map cannot be read; the page
    then falls back to a neutral colour per species.
    """
    try:
        wm = gis.content.get(TOTARA_WEBMAP_ID).get_data() or {}
    except Exception:
        log.exception("  Could not read the Tōtara web map — species colours unavailable")
        return {}

    def walk(layers, parent=None):
        for lyr in layers or []:
            yield lyr, parent
            yield from walk(lyr.get("layers"), lyr)

    points = tracks = group = None
    for lyr, parent in walk(wm.get("operationalLayers")):
        url = str(lyr.get("url") or "")
        if "Contractor_Data/FeatureServer" not in url:
            continue
        if url.endswith(f"/{PEST_PLANT_POINTS_LAYER}"):
            points, group = lyr, parent
        elif url.endswith(f"/{PEST_PLANT_TRACKS_LAYER}"):
            tracks = lyr
    if points is None:
        log.warning("  Pest plant layer not found in the Tōtara web map — species colours unavailable")
        return {}

    renderer = ((points.get("layerDefinition") or {}).get("drawingInfo") or {}).get("renderer") or {}
    colours = {}
    for info in renderer.get("uniqueValueInfos") or []:
        hexcol = _rgba_to_hex((info.get("symbol") or {}).get("color"))
        if info.get("value") and hexcol:
            colours[str(info["value"])] = hexcol

    size = next((v for v in renderer.get("visualVariables") or []
                 if v.get("type") == "sizeInfo" and v.get("field")), {})

    # Tracks are a CIM line: the last solid stroke is the fill colour on top of
    # the casing beneath it.
    track_colour = None
    track_casing = None
    t_sym = (((tracks or {}).get("layerDefinition") or {}).get("drawingInfo") or {}) \
        .get("renderer", {}).get("symbol", {}).get("symbol", {})
    strokes = [s for s in t_sym.get("symbolLayers") or [] if s.get("type") == "CIMSolidStroke"]
    if strokes:
        track_colour = _rgba_to_hex(strokes[0].get("color"))
        if len(strokes) > 1:
            track_casing = _rgba_to_hex(strokes[-1].get("color"))

    log.info(f"  Web map symbology: {len(colours)} species colours, size by {size.get('field')}")
    return {
        "colours": colours,
        "sizeKey": {
            "field":    size.get("field"),
            "minValue": size.get("minDataValue"),
            "maxValue": size.get("maxDataValue"),
            "minSize":  size.get("minSize"),
            "maxSize":  size.get("maxSize"),
        } if size else None,
        "track": {"colour": track_colour, "casing": track_casing},
        "titles": {
            "group":  (group or {}).get("title"),
            "points": points.get("title"),
            "tracks": (tracks or {}).get("title"),
        },
    }


def _control_outcome(note) -> str:
    """Bucket the free-text Control_notes by its leading phrase."""
    text = note.strip().casefold()
    if text.startswith("controlled"):
        return "controlled"
    if text.startswith("partially"):
        return "partial"
    if text.startswith("not controlled"):
        return "notControlled"
    return "unrecorded"   # blank, 'Spotted', anything else


def extract_pest_plant_data() -> dict | None:
    """
    Pest plant control figures for the three pest-plant-*.html embeds.

    Waypoints: one row per weed location — SpeciesID, FinYr ('24-25'), Date
    (dd/mm/yyyy string), Size_sqm, Age_class (A/J/S), Control_notes (free text,
    bucketed by its leading phrase), RPMPspecies ('Y' or blank).
    Tracks: Distance_Km per GPS track walked.

    Reports the last completed FY, falling back to the newest FY with records.
    Returns None on failure or an empty layer, so the pages keep last good data.
    """
    log.info("Processing pest plant data...")

    try:
        from arcgis.gis import GIS
        gis = GIS("pro")
        log.info(f"  Connected to AGOL as: {gis.properties.user.username}")
    except Exception:
        log.exception("  Cannot connect to AGOL — pest plant data skipped")
        return None

    try:
        item = gis.content.get(PEST_PLANT_ITEM_ID)
        svc  = item.url
        wp = _fetch_agol_df(gis, svc, PEST_PLANT_POINTS_LAYER)
        pl = _fetch_agol_df(gis, svc, PEST_PLANT_TRACKS_LAYER)
    except Exception:
        log.exception("  Pest plant layer query failed — pest plant data skipped")
        return None

    if wp.empty:
        log.warning("  Pest plant waypoints came back empty — keeping the last good page data")
        return None

    symbology = _fetch_pest_plant_symbology(gis)
    colours   = {k.casefold(): v for k, v in (symbology.get("colours") or {}).items()}

    wp["SpeciesID"] = wp["SpeciesID"].fillna("Unrecorded").astype(str).str.strip()
    wp["_outcome"]  = wp["Control_notes"].fillna("").astype(str).map(_control_outcome)
    wp["_age"]      = wp["Age_class"].fillna("").astype(str).str.upper().str.strip()
    wp["_date"]     = pd.to_datetime(wp["Date"], dayfirst=True, errors="coerce")
    wp["_rpmp"]     = wp["RPMPspecies"].fillna("").astype(str).str.upper().isin(["Y", "YES"])
    wp["Size_sqm"]  = pd.to_numeric(wp["Size_sqm"], errors="coerce").fillna(0)
    km_col = "Distance_Km" if "Distance_Km" in pl.columns else None
    if km_col:
        pl[km_col] = pd.to_numeric(pl[km_col], errors="coerce").fillna(0)

    fys = sorted(wp["FinYr"].dropna().unique())

    def summarise(w: pd.DataFrame, p: pd.DataFrame) -> dict:
        outcome = w["_outcome"].value_counts()
        age     = w["_age"].value_counts()
        n       = int(len(w))
        controlled = int(outcome.get("controlled", 0))
        # % controlled is of records with an outcome noted — 22-23 left 206 notes
        # blank, which would otherwise read as a collapse in control.
        noted = n - int(outcome.get("unrecorded", 0))
        return {
            "records":       n,
            "species":       int(w["SpeciesID"].nunique()),
            "areaSqm":       int(round(float(w["Size_sqm"].sum()))),
            "km":            round(float(p[km_col].sum()), 1) if km_col and not p.empty else None,
            "tracks":        int(len(p)),
            "days":          int(w["_date"].dt.date.nunique()),
            "rpmp":          int(w["_rpmp"].sum()),
            "controlled":    controlled,
            "partial":       int(outcome.get("partial", 0)),
            "notControlled": int(outcome.get("notControlled", 0)),
            "unrecorded":    int(outcome.get("unrecorded", 0)),
            "controlledPct": round(controlled / noted * 100) if noted else None,
            "age":           {k: int(age.get(k, 0)) for k in ("A", "J", "S")},
        }

    by_fy = []
    for fy in fys:
        s = summarise(wp[wp["FinYr"] == fy], pl[pl["FinYr"] == fy] if "FinYr" in pl.columns else pl)
        s["fy"] = fy
        by_fy.append(s)

    wanted, _ = previous_financial_year()
    fy = latest_fy_with_records(wp, "FinYr", wanted, "pest plant records")
    current = next(s for s in by_fy if s["fy"] == fy)

    # One row per species, most records first. Counts and area per FY let the
    # page switch years without another run.
    species = []
    for name, grp in wp.groupby("SpeciesID"):
        colour = colours.get(name.casefold())
        if colour is None:
            log.warning(f"  '{name}' has no symbol in the web map — it does not draw on the map")
        species.append({
            "name":      name,
            "colour":    colour,
            "rpmp":      bool(grp["_rpmp"].any()),
            "total":     int(len(grp)),
            "areaSqm":   int(round(float(grp["Size_sqm"].sum()))),
            "byFy":      {f: int((grp["FinYr"] == f).sum()) for f in fys},
            "areaByFy":  {f: int(round(float(grp.loc[grp["FinYr"] == f, "Size_sqm"].sum()))) for f in fys},
        })
    species.sort(key=lambda s: -s["total"])

    log.info(
        f"  FY {fy}: {current['records']} records, {current['species']} species, "
        f"{current['areaSqm']:,} m², {current['km']} km, {current['controlledPct']}% controlled"
    )

    fy_start = 2000 + int(fy[:2])
    now = datetime.datetime.now()
    return {
        "generated": f"{now.day} {now:%b %Y}",
        "fy":        fy,
        "fyLabel":   f"{fy_start}-{fy[3:]}",
        "fyWanted":  wanted,
        "fys":       fys,
        "contractor": wp["Cont_name"].mode().iloc[0] if "Cont_name" in wp.columns and wp["Cont_name"].notna().any() else None,
        "current":   current,
        "byFy":      by_fy,
        "allYears":  summarise(wp, pl),
        "species":   species,
        "sizeKey":   symbology.get("sizeKey"),
        "track":     symbology.get("track") or {"colour": None, "casing": None},
        "titles":    symbology.get("titles") or {},
    }


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
        log.warning(f"Marker /* {start_marker} */ not found — skipping")
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


def inject_into_pest_plant_html(data: dict) -> None:
    block = f"const PEST_PLANT_DATA = {json.dumps(data, indent=2, ensure_ascii=False)};"
    for path in PEST_PLANT_HTML_PATHS:
        if not path.exists():
            log.warning(f"{path} does not exist — skipping.")
            continue
        html = path.read_text(encoding="utf-8")
        html = _replace_block(html, "PEST_PLANT_DATA_START", "PEST_PLANT_DATA_END", block)
        path.write_text(html, encoding="utf-8")
        log.info(f"Updated {path}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Tōtara Reserve dashboard data export")
    parser.add_argument(
        "--only", choices=["river", "pco", "plants"],
        help="Run one section only. Default runs all three.",
    )
    only = parser.parse_args().only

    log.info("=== Totara Reserve Data Export ===")

    if only in (None, "river"):
        log.info("--- LAWA Recreational Water Quality ---")
        df   = _get_lawa_dataframe()
        swim = extract_swim_data(df)

        log.info("--- Hilltop River Level / Flow ---")
        river = extract_river_data()

        log.info("--- Injecting into river-management.html ---")
        inject_into_html(swim, river)

    if only in (None, "pco"):
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

    if only in (None, "plants"):
        log.info("--- Pest Plant Control (requires ArcGIS Pro env) ---")
        plants = extract_pest_plant_data()
        if plants:
            inject_into_pest_plant_html(plants)
        else:
            log.warning("Pest plant data unavailable — pest-plant-*.html not updated.")

    log.info("=== Done ===")


if __name__ == "__main__":
    main()
