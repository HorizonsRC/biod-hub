"""
KKT_Dashboard_Export.py
=======================

Queries the Biodiversity_KKT_Projects service on AGOL and writes
html/kkt/dashboard_data.json, which the KKT chart pages fetch at runtime.
Optionally commits and pushes it to GitHub Pages.

Same pattern as PM_Dashboard_Export.py: the HTML chart files are static and
never regenerated — only this JSON changes.

Run after KKT_Stats_Update.py, from the ArcGIS Pro Python environment, signed
in to the portal in ArcGIS Pro:

    python KKT_Dashboard_Export.py            # write the JSON only
    python KKT_Dashboard_Export.py --push     # write, commit and push
"""

import os
import re
import json
import logging
import argparse
import subprocess
from datetime import datetime as dt

from config import KKT_SERVICE_URL

KKT_LAYER_ID = 4
KKT_STATS_ID = 5

# Fields carried into the JSON. Kept deliberately narrow — this file is served
# publicly from GitHub Pages, so only what the charts actually need goes in it.
PROJECT_FIELDS = [
    "GlobalID", "Grant_year_1", "Group_name_1", "ProjectNam_1", "District",
    "ProjectType", "Current_YearFund", "Prev_Act_Grant_1", "No_of_GrantYears",
]

# The four activity slots and their funding. Read here, but not carried into the
# JSON as eight loose fields — they are folded into an "activities" list per
# project instead, which is what the by-type chart actually wants.
TYPE_FIELDS = ["ProjectType", "ProjectType_2", "ProjectType_3", "ProjectType_4"]
FUND_FIELDS = ["ProjectType_fund", "ProjectType_fund2", "ProjectType_fund3",
               "ProjectType_fund4"]
STATS_FIELDS = [
    "ProjectID", "Grant_year", "Applicant", "Plants_planted", "No_plantsReleased",
    "Pest_Animal_Control", "Traps_funded", "Total_Project_Traps",
    "Pest_Plant_Control_m2", "Total_Pest_Plant__area_controll",
    "Restoration_area_supported_Ha", "Community_members", "Community_hours",
    "Volunteers", "Fencing_LnMtr",
]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs", "kkt")
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, dt.now().strftime('%Y-%m-%d_%H-%M-%S') + '_export.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.FileHandler(log_file, encoding='utf-8'),
              logging.StreamHandler()],
)
log = logging.getLogger(__name__)


def parse_money(value):
    """'$3,763.00 ' -> 3763.0. Returns None if there's no number in there."""
    if value is None:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", str(value))
    if cleaned in ("", ".", "-"):
        return None
    try:
        return round(float(cleaned), 2)
    except ValueError:
        return None


def norm_guid(guid):
    """{UPPERCASE-WITH-BRACES}. The layer returns GlobalIDs bare and lowercase
    but the stats table stores them braced and uppercase, so both sides have to
    be normalised before they will compare equal."""
    if not guid:
        return ""
    return "{" + str(guid).strip().strip("{}").upper() + "}"


def clean(attrs, fields):
    """Keep only the wanted fields, and drop empty strings so charts see null."""
    out = {}
    for f in fields:
        v = attrs.get(f)
        if isinstance(v, str):
            v = v.strip() or None
        out[f] = v
    return out


def main():
    ap = argparse.ArgumentParser(description="Build the KKT dashboard JSON.")
    ap.add_argument("--push", action="store_true",
                    help="Commit and push the JSON to GitHub Pages.")
    args = ap.parse_args()

    log.info("=" * 70)
    log.info("KKT DASHBOARD EXPORT")
    log.info("=" * 70)

    from arcgis.gis import GIS
    from arcgis.features import FeatureLayerCollection

    log.info("Connecting to AGOL using the ArcGIS Pro sign-in...")
    gis = GIS("home")
    flc = FeatureLayerCollection(KKT_SERVICE_URL, gis=gis)
    layer = next(l for l in flc.layers if l.properties.id == KKT_LAYER_ID)
    stats = next(t for t in flc.tables if t.properties.id == KKT_STATS_ID)

    log.info(f"Reading {layer.properties.name}...")
    projects = []
    no_activities = 0
    unallocated = 0
    for f in layer.query(where="1=1",
                         out_fields=",".join(PROJECT_FIELDS + TYPE_FIELDS
                                             + FUND_FIELDS),
                         return_geometry=False).features:
        a = f.attributes
        row = clean(a, PROJECT_FIELDS)
        # Funding is stored as text like '$3,763.00 ' — give the charts a number
        # to sum as well, rather than making every page re-parse it.
        row["funding"] = parse_money(row.get("Current_YearFund"))

        # One entry per activity the project carries, with the grant money the
        # team recorded against it. A fund of 0 is meaningful — the activity
        # happened but no grant funding went to it — so it is kept, not dropped.
        acts = []
        for tf, ff in zip(TYPE_FIELDS, FUND_FIELDS):
            t = a.get(tf)
            t = t.strip() if isinstance(t, str) else t
            if not t:
                continue
            v = a.get(ff)
            acts.append({"type": t, "fund": None if v is None else round(float(v), 2)})
        row["activities"] = acts

        if not acts:
            no_activities += 1
        else:
            # The parts deliberately need not add up to the grant: the team
            # allocated only money actually spent. Counted, not corrected.
            got = sum(x["fund"] or 0 for x in acts)
            if row["funding"] and abs(got - row["funding"]) > 0.011:
                unallocated += 1

        projects.append(row)
    log.info(f"  {len(projects)} projects")
    slot_use = [sum(1 for p in projects if len(p["activities"]) > i) for i in range(4)]
    log.info(f"  Activity slot use (1-4): {slot_use}")
    if no_activities:
        log.warning(f"  {no_activities} project(s) carry no activity at all — these "
                    f"are invisible to the by-type chart.")
    log.info(f"  {unallocated} project(s) allocate less than their grant. Expected — "
             f"the team recorded only money actually spent per activity.")

    log.info(f"Reading {stats.properties.name}...")
    stats_rows = [clean(f.attributes, STATS_FIELDS)
                  for f in stats.query(where="1=1",
                                       out_fields=",".join(STATS_FIELDS)).features]
    log.info(f"  {len(stats_rows)} stats rows")

    # Join each stats row to its project so charts can group by the canonical
    # Group_name_1. The stats table's own Applicant field is free text copied
    # from the spreadsheet, so the same group is spelled differently year to
    # year ("Eco School" / "The Eco School") and would split into two bars.
    by_guid = {norm_guid(p["GlobalID"]): p for p in projects}
    unlinked = 0
    for r in stats_rows:
        p = by_guid.get(norm_guid(r.get("ProjectID")))
        if p:
            r["group_name"]   = p.get("Group_name_1")
            r["project_name"] = p.get("ProjectNam_1")
            r["district"]     = p.get("District")
        else:
            unlinked += 1
            r["group_name"] = r.get("Applicant")
            r["project_name"] = None
            r["district"] = None
    if unlinked:
        log.warning(f"  {unlinked} stats row(s) link to no project — these fall back "
                    f"to the spreadsheet's Applicant text for grouping.")

    missing_year = sum(1 for r in stats_rows if not r.get("Grant_year"))
    if missing_year:
        log.warning(f"  {missing_year} stats row(s) have no Grant_year — these will "
                    f"be hidden by the year filters. Run "
                    f"KKT_Stats_Update.py --backfill-years --push to fix.")

    years = sorted({r["Grant_year"] for r in stats_rows if r.get("Grant_year")})
    log.info(f"  Stats years present: {years}")
    log.info(f"  Project years present: "
             f"{sorted({p['Grant_year_1'] for p in projects if p.get('Grant_year_1')})}")

    generated_at = dt.now().strftime('%d %B %Y %H:%M')
    payload = {
        "generated_at": generated_at,
        "projects": projects,
        "stats": stats_rows,
    }

    out_path = os.path.join(SCRIPT_DIR, "html", "kkt", "dashboard_data.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)
    log.info(f"Written: {out_path}  ({os.path.getsize(out_path) / 1024:.0f} KB)")

    if args.push:
        log.info("Committing and pushing to GitHub...")
        rel = "html/kkt/dashboard_data.json"
        subprocess.run(['git', '-C', SCRIPT_DIR, 'add', rel], check=True)
        result = subprocess.run(
            ['git', '-C', SCRIPT_DIR, 'commit', '-m',
             f'Update KKT dashboard data ({generated_at})'],
            capture_output=True, text=True)
        if result.returncode == 0:
            subprocess.run(['git', '-C', SCRIPT_DIR, 'push', 'origin', 'main'],
                           check=True)
            log.info("Pushed to GitHub successfully.")
        else:
            log.info("No changes to commit — dashboard data already up to date.")
    else:
        log.info("Not pushed (no --push). The charts read the copy on GitHub Pages, "
                 "so push when you want the live dashboard to update.")

    log.info("=" * 70)
    log.info(f"Generated at : {generated_at}")
    log.info(f"Log          : {log_file}")
    log.info("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
