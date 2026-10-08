"""
Targeted Rates dashboard data export.

Reads the Biodiversity Targeted Rates contractor data view on AGOL and writes
the chart data into the two Targeted Rates pages:

    html/targeted-rates/WBP.html   Waitārere Beach Community Project
    html/targeted-rates/REG.html   Rangitīkei Environment Group

Marker comments the script rewrites:
    WBP.html:  /* WBP_DATA_START */  /* WBP_DATA_END */
    REG.html:  /* REG_DATA_START */  /* REG_DATA_END */

REG's 2024-25 figures come from their annual report, not spatial data (their
spatial data for that season is too patchy to chart). They sit in REG.html
outside the marker block, so this script never touches them, and any spatial
points for a REPORT_SEASONS year are left out.

Does not commit or push. Run_All_Updates.py does that.

Usage (ArcGIS Pro Python environment):
    python Targeted_Rates_Data_Export.py
"""

import datetime
import json
import logging
import re
import sys
from pathlib import Path

import pandas as pd

try:
    import config
except ImportError:
    config = None

# ── Paths ─────────────────────────────────────────────────────────────────────
HERE     = Path(__file__).parent
WBP_HTML = HERE / "html" / "targeted-rates" / "WBP.html"
REG_HTML = HERE / "html" / "targeted-rates" / "REG.html"

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR = HERE / "logs" / "targeted-rates"
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_path = LOG_DIR / f"targeted_rates_{datetime.datetime.now():%Y%m%d_%H%M%S}.log"

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
WAYPOINTS_LAYER = 1

WBP_SITE = "Waitarere Beach"
# Charted on their own; every other species is grouped as "Other".
WBP_MAIN_SPECIES = ["Tree Lupin", "Sydney Golden Wattle"]

REG_CONTRACTOR = "Rangitikei Environment Group"
REG_SPECIES    = "old man's beard"   # compared casefolded, curly apostrophe straightened
REPORT_SEASONS = {"24-25"}           # shown from REG's report, held in REG.html

# The same reserve recorded under two names. Left: as entered; right: chart name.
RESERVE_ALIASES = {
    "Mount Stewart Reserve":  "Mount Stewart",
    "Sutherland Puriri Bush": "Sutherlands Puriri Reserve",
}

# REG converted their size classes: small = S, medium = J, large = A.
REG_AGES = {"S": "seedling", "J": "juvenile", "A": "adult"}
REG_AGE_KEYS = ["seedling", "juvenile", "adult", "notRecorded"]

FY_RE = re.compile(r"^(\d{2})-(\d{2})$")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fy_ok(value) -> bool:
    """True for a financial year like '25-26' (second year follows the first)."""
    m = FY_RE.match(value) if isinstance(value, str) else None
    return bool(m) and (int(m[1]) + 1) % 100 == int(m[2])


def _fy_key(fy: str) -> int:
    return int(fy[:2])


def clean_seasons(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """Drop rows whose FinYr is not a real financial year, and say so.

    A typo such as '24-26' would otherwise become a phantom season on every chart.
    """
    df = df.copy()
    df["FinYr"] = df["FinYr"].astype("string").str.strip()
    ok = df["FinYr"].map(_fy_ok).astype(bool)
    for value, n in df.loc[~ok, "FinYr"].value_counts(dropna=False).items():
        log.warning(f"{label}: {n} point(s) with FinYr {value!r} skipped, expected e.g. 25-26")
    return df[ok]


def _control_outcome(notes) -> str:
    """Sort a Control_notes entry into one of the chart's three outcomes."""
    n = notes.casefold() if isinstance(notes, str) else ""
    if "partially controlled" in n:
        return "partial"
    if "not controlled" in n:
        return "notControlled"
    if "controlled" in n:
        return "controlled"
    return "notRecorded"


# ── AGOL ──────────────────────────────────────────────────────────────────────

def fetch_waypoints() -> pd.DataFrame:
    url = getattr(config, "TARGETED_RATES_CONTRACTOR_URL", None)
    if not url:
        raise SystemExit("TARGETED_RATES_CONTRACTOR_URL is not set in config.py")

    from arcgis.gis import GIS
    from arcgis.features import FeatureLayer

    gis = GIS("pro")
    log.info(f"Connected to AGOL as: {gis.properties.user.username}")
    df = FeatureLayer(f"{url}/{WAYPOINTS_LAYER}", gis).query(
        where="1=1", return_geometry=False, as_df=True)
    log.info(f"Waypoints: {len(df)} rows")
    return df


# ── Waitārere Beach ───────────────────────────────────────────────────────────

def build_wbp(df: pd.DataFrame) -> dict:
    w = df[df["SiteName"] == WBP_SITE]
    if w.empty:
        raise RuntimeError(f"No '{WBP_SITE}' points returned, refusing to blank WBP.html")
    w = clean_seasons(w, "WBP")

    seasons = sorted(w["FinYr"].unique(), key=_fy_key)
    groups = WBP_MAIN_SPECIES + ["Other"]
    w = w.assign(
        group=w["SpeciesID"].where(w["SpeciesID"].isin(WBP_MAIN_SPECIES), "Other"),
        outcome=w["Control_notes"].map(_control_outcome),
    )

    by_season = (pd.crosstab(w["group"], w["FinYr"])
                 .reindex(index=groups, columns=seasons, fill_value=0))
    control = (pd.crosstab(w["outcome"], w["FinYr"])
               .reindex(columns=seasons, fill_value=0))

    unrecorded = int(control.loc["notRecorded"].sum()) if "notRecorded" in control.index else 0
    if unrecorded:
        log.warning(f"WBP: {unrecorded} point(s) with no control outcome in Control_notes")

    other_ages = w.loc[~w["Age_class"].isin(["S", "J", "A"]), "Age_class"]
    if len(other_ages):
        log.warning(f"WBP: {len(other_ages)} point(s) with no S/J/A age class: "
                    f"{other_ages.value_counts(dropna=False).to_dict()}")

    summary = []
    for g in groups:
        rows = w[w["group"] == g]
        item = {
            "name":    g,
            "plants":  int(len(rows)),
            "sqm":     int(round(rows["Size_sqm"].fillna(0).sum())),
            "seedJuv": int(rows["Age_class"].isin(["S", "J"]).sum()),
            "adult":   int((rows["Age_class"] == "A").sum()),
        }
        if g == "Other":
            item["species"] = rows["SpeciesID"].value_counts().index.tolist()
        summary.append(item)

    for fy in seasons:
        log.info(f"WBP {fy}: {int(by_season[fy].sum())} plants")

    return {
        "seasons":   seasons,
        "groups":    groups,
        "bySeason":  {g: [int(v) for v in by_season.loc[g]] for g in groups},
        "control":   {k: [int(v) for v in control.loc[k]] if k in control.index
                      else [0] * len(seasons)
                      for k in ("controlled", "partial", "notControlled")},
        "summary":   summary,
    }


# ── Rangitīkei Environment Group ──────────────────────────────────────────────

def build_reg(df: pd.DataFrame) -> dict:
    r = df[df["Cont_name"] == REG_CONTRACTOR]
    if r.empty:
        raise RuntimeError(f"No '{REG_CONTRACTOR}' points returned, refusing to blank REG.html")

    species = (r["SpeciesID"].astype("string").fillna("")
               .str.replace("’", "'").str.strip().str.casefold())
    others = r.loc[species != REG_SPECIES, "SpeciesID"]
    if len(others):
        log.info(f"REG: {len(others)} point(s) of other species left out: "
                 f"{others.value_counts(dropna=False).to_dict()}")
    r = clean_seasons(r[species == REG_SPECIES], "REG")

    in_report = r["FinYr"].isin(REPORT_SEASONS)
    if in_report.any():
        log.info(f"REG: {int(in_report.sum())} point(s) in {sorted(REPORT_SEASONS)} left out, "
                 f"that season shows the REG report figures")
    r = r[~in_report]

    reserve = r["SiteName"].astype("string").fillna("").str.strip()
    if (reserve == "").any():
        log.warning(f"REG: {int((reserve == '').sum())} point(s) with no SiteName")
    r = r.assign(
        reserve=reserve.replace("", "No site name").replace(RESERVE_ALIASES),
        age=r["Age_class"].map(REG_AGES).fillna("notRecorded"),
    )

    seasons = {}
    for fy in sorted(r["FinYr"].unique(), key=_fy_key):
        g = r[r["FinYr"] == fy]
        ct = pd.crosstab(g["reserve"], g["age"]).reindex(columns=REG_AGE_KEYS, fill_value=0)
        seasons[fy] = {
            "reserves": ct.index.tolist(),
            **{k: [int(v) for v in ct[k]] for k in REG_AGE_KEYS},
            "points": int(len(g)),
        }
        log.info(f"REG {fy}: {len(g)} points across {len(ct)} reserves, "
                 f"{int(ct['notRecorded'].sum())} with no age class")

    if not seasons:
        log.warning("REG: no spatial seasons to chart, page will show the report season only")
    return {"seasons": seasons}


# ── HTML injection ────────────────────────────────────────────────────────────

def inject(path: Path, marker: str, const_name: str, data: dict) -> None:
    """Replace the marker block in one page. Fails if the markers are missing."""
    html = path.read_text(encoding="utf-8")
    block = f"const {const_name} = {json.dumps(data, indent=2, ensure_ascii=False)};"
    pattern = rf"(/\* {marker}_START \*/\n).*?(\n\s*/\* {marker}_END \*/)"
    new, n = re.subn(pattern, lambda m: m.group(1) + block + m.group(2), html, flags=re.DOTALL)
    if n != 1:
        raise RuntimeError(f"{path.name}: expected one /* {marker}_START */ block, found {n}")
    if new == html:
        log.info(f"{path.name}: data unchanged")
        return
    path.write_text(new, encoding="utf-8")
    log.info(f"Updated {path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=== Targeted Rates Data Export ===")
    df = fetch_waypoints()

    log.info("--- Waitārere Beach ---")
    inject(WBP_HTML, "WBP_DATA", "WBP_DATA", build_wbp(df))

    log.info("--- Rangitīkei Environment Group ---")
    inject(REG_HTML, "REG_DATA", "REG_DATA", build_reg(df))

    log.info(f"=== Done === Log: {log_path}")


if __name__ == "__main__":
    main()
