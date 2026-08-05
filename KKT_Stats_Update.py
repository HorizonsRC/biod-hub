"""
KKT_Stats_Update.py
===================

Loads the team's annual "Summary KKT stats" spreadsheet into the
KKT_Related_Table_Statistics table on AGOL, matching each spreadsheet row to
its project feature in KKT_Projects_Layer for the given grant year.

The layer/table relationship is 1:1 —
    KKT_Projects_Layer.GlobalID (origin)  ->  KKT_Related_Table_Statistics.ProjectID
Each layer feature is one project *for one grant year*, so one stats row per
feature is correct and no schema change is needed.

Run order
---------
1.  Dry run (default) — matches names, writes a review CSV, changes nothing:
        python KKT_Stats_Update.py --year 25_26
2.  Check the review CSV. Fix any UNMATCHED / AMBIGUOUS rows by adding an entry
    to NAME_ALIASES or ROW_OVERRIDES below, then re-run the dry run.
3.  When the review CSV looks right, apply it:
        python KKT_Stats_Update.py --year 25_26 --push

Run from the ArcGIS Pro Python environment and be signed in to the portal in
ArcGIS Pro first (the script authenticates with GIS("home")).
"""

import os
import re
import sys
import csv
import math
import logging
import argparse
import unicodedata
from datetime import datetime as dt
from difflib import SequenceMatcher

import pandas as pd

from config import KKT_SERVICE_URL, KKT_STATS_XLSX, KKT_OUTPUT_DIR

KKT_LAYER_ID = 4   # KKT_Projects_Layer
KKT_STATS_ID = 5   # KKT_Related_Table_Statistics

# Grant year field on the stats table (text, 5 chars, e.g. "25_26"). Mirrors
# Grant_year_1 on the projects layer. Without it nothing downstream can split
# the stats by financial year, since the table has no other year marker.
YEAR_FIELD = "Grant_year"

# Rows in the spreadsheet that are not projects
SKIP_APPLICANTS = {"totals", "total", "grand total"}

# Fuzzy-match confidence thresholds
MATCH_ACCEPT = 0.78   # below this a row is reported UNMATCHED
MATCH_MARGIN = 0.06   # if 1st and 2nd best are this close, report AMBIGUOUS


# ============================================================
# SPREADSHEET -> RELATED TABLE FIELD MAP
# ============================================================
# Left  = column header in the team's spreadsheet
# Right = (field name in KKT_Related_Table_Statistics, cast)
#
# cast: "int"  -> whole number      "float" -> decimal
#       "str"  -> text (the AGOL field is a String even where the value is a
#                 number, so it is written as text)
#
# NOTE — two mappings below are assumptions flagged for review:
#   * "# Plants funded" -> Plants_planted   (funded vs actually planted)
#   * "Pest Plant Control ha" -> Pest_Plant_Control_m2 with a x10,000 ha->m2
#     conversion, because the AGOL field is named m2 but the sheet is in ha.
#     Set PEST_PLANT_HA_TO_M2 = False to write the raw hectare value instead.
PEST_PLANT_HA_TO_M2 = True

FIELD_MAP = {
    "Applicant":                              ("Applicant",                       "str"),
    "Plants grown":                           ("Plants_grown",                    "str"),
    "# Plants funded":                        ("Plants_planted",                  "int"),
    "# plants released":                      ("No_plantsReleased",               "int"),
    "# Pest Animal Controlled":               ("Pest_Animal_Control",             "int"),
    "Traps funded":                           ("Traps_funded",                    "int"),
    "Total Project Traps":                    ("Total_Project_Traps",             "float"),
    "Total PROJECT area (ha)":                ("Total_PROJECT_area",              "str"),
    "Pest Plant Control #stems":              ("Pest_Plant_Control__stems",       "str"),
    "Pest Plant Control ha":                  ("Pest_Plant_Control_m2",           "int"),
    "Total Pest Plant  area controlled  #ha": ("Total_Pest_Plant__area_controll", "float"),
    "Monitoring":                             ("Monitoring",                      "str"),
    "Education # of sessions":                ("Education",                       "str"),
    "Fencing LnMtr":                          ("Fencing_LnMtr",                   "str"),
    "Species Translocation":                  ("Species_Translocation",           "str"),
    "Restoration area supported Ha":          ("Restoration_area_supported_Ha",   "float"),
    "Community members engaged":              ("Community_members",               "int"),
    "Community hrs":                          ("Community_hours",                 "int"),
    "Volunteers":                             ("Volunteers",                      "int"),
    "NEW Assets purchased (not traps)":       ("NEW_Assets_purchased__not_traps", "str"),
    "Extra Info":                             ("Extra_Info",                      "str"),
}

# Spreadsheet columns with nowhere to go in the related table.
# "Catchment" has no matching AGOL field — add one to the table if it is wanted.
UNMAPPED_COLUMNS = {"Catchment"}


# ============================================================
# NAME MATCHING OVERRIDES
# ============================================================
# NAME_ALIASES — spreadsheet group name -> Group_name_1 in KKT_Projects_Layer.
# Use for groups the fuzzy matcher gets wrong or cannot reach. Keys are
# normalised (lowercase, macrons stripped, punctuation removed) before lookup,
# so you can type them however they appear in the sheet.
NAME_ALIASES = {
    "the eco school":                        "Eco School",
    "owhango alive society incorporated":     "Ōwhanga Alive",
    "raetihi school":                         "Raetihi Primary School",
    "kopua bush":                             "Kopua Bush Remnant Management Group",
    "ruahine whio":                           "Ruahine Whio Protection Trust",
    "ngati kahungunu":                        "Ngati Kahungunu ki Tamaki-nui-a-Rua Trust",
    "manawatu river catchment collective":    "Manawatū River Catchments Collective",
    "ohaumoko trust":                         "Ohaumoko Trust",
}

# ROW_OVERRIDES — for groups that run several projects in one year, where the
# spreadsheet gives no project name to tell them apart. Keyed by grant year,
# then by the spreadsheet row number shown in the review CSV (excel_row),
# and the value is the exact ProjectNam_1 from KKT_Projects_Layer.
ROW_OVERRIDES = {
    "25_26": {
        # Ngāwakahiamoe Bush Trust — told apart by the figures: row 24 has the
        # 440 plants and the 0.45 Ha, row 25 has the traps.
        24: "Fence off and plant 0.45Ha of pasture and mixed podocarps.",
        25: "Predator Control Project",
        # Waitarere Rise — row 41 is planting, row 42 is the trapping figures.
        41: "Waitarere Rise Reserve riparian margin",
        42: "Waitarere Rise Reserve trapping",
        # Ihaia Taueki Trust rows 12 and 13 cannot be told apart from the
        # spreadsheet — both are wetland restoration with the same area and
        # community figures. Confirm with the team which is which and set:
        # 12: "Inanga-riki Lagoon - A6B Restoration of Wetlands",
        # 13: "Pakau-hokio-iti A6B - Restoration of Wetland",
    },
}


# ============================================================
# LOGGING
# ============================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs", "kkt")
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, dt.now().strftime('%Y-%m-%d_%H-%M-%S') + '_kkt_stats.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


# ============================================================
# HELPERS
# ============================================================

def normalise(text):
    """Lowercase, strip macrons/accents, drop punctuation and filler words.

    Used only for comparing names — the original text is always what gets
    written to AGOL.
    """
    if text is None or (isinstance(text, float) and math.isnan(text)):
        return ""
    s = unicodedata.normalize("NFKD", str(text))
    s = "".join(c for c in s if not unicodedata.combining(c))   # ā -> a
    s = s.lower()
    s = re.sub(r"\(.*?\)", " ", s)          # drop bracketed acronyms: (WBEG)
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\b(the|inc|incorporated|society|branch)\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()


def norm_guid(guid):
    """Put a GlobalID in the {UPPERCASE-WITH-BRACES} form.

    The layer query returns GlobalIDs bare and lowercase, but the stats table
    stores them braced and uppercase — which is the form the relationship needs.
    Without this, existing rows look unmatched and get duplicated.
    """
    if not guid:
        return ""
    return "{" + str(guid).strip().strip("{}").upper() + "}"


def split_applicant(applicant):
    """Spreadsheet applicants are sometimes "Group - Project name".

    Returns (group_part, project_part). project_part is "" when absent.
    """
    parts = str(applicant).split(" - ", 1)
    if len(parts) == 2 and len(parts[1].strip()) > 3:
        return parts[0].strip(), parts[1].strip()
    return str(applicant).strip(), ""


def to_int(v):
    if v is None or (isinstance(v, float) and math.isnan(v)) or str(v).strip() == "":
        return None
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return None


def to_float(v):
    if v is None or (isinstance(v, float) and math.isnan(v)) or str(v).strip() == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def to_str(v):
    if v is None or (isinstance(v, float) and math.isnan(v)) or str(v).strip() == "":
        return None
    # Numbers that live in String fields shouldn't arrive as "650.0"
    if isinstance(v, float) and float(v).is_integer():
        return str(int(v))
    return str(v).strip()


CASTS = {"int": to_int, "float": to_float, "str": to_str}


def year_from_filename(path):
    """"Summary KKT stats 2025-26.xlsx" -> "25_26". Returns None if unclear."""
    m = re.search(r"(20)?(\d{2})\s*[-_/]\s*(\d{2})", os.path.basename(path))
    return f"{m.group(2)}_{m.group(3)}" if m else None


# ============================================================
# READ THE SPREADSHEET
# ============================================================

def read_spreadsheet(path):
    log.info(f"Reading spreadsheet: {path}")
    df = pd.read_excel(path, sheet_name=0)
    df.columns = [str(c).strip() for c in df.columns]

    applicant_col = df.columns[0]
    df["_excel_row"] = df.index + 2          # +2 = header row plus 1-based Excel rows

    before = len(df)
    df = df[df[applicant_col].notna()]
    df = df[~df[applicant_col].astype(str).str.strip().str.lower().isin(SKIP_APPLICANTS)]
    log.info(f"  {len(df)} project rows ({before - len(df)} blank/total rows skipped)")

    # Warn about anything in the sheet the field map doesn't know about — a new
    # column added by the team should not be silently dropped.
    known = set(FIELD_MAP) | UNMAPPED_COLUMNS | {"_excel_row"}
    unknown = [c for c in df.columns if c not in known]
    if unknown:
        log.warning(f"  Spreadsheet columns not in FIELD_MAP (ignored): {unknown}")
    missing = [c for c in FIELD_MAP if c not in df.columns]
    if missing:
        log.warning(f"  FIELD_MAP columns not found in this spreadsheet: {missing}")

    return df, applicant_col


# ============================================================
# MATCH SPREADSHEET ROWS TO PROJECT FEATURES
# ============================================================

def match_rows(df, applicant_col, projects, year):
    """Assign each spreadsheet row to one project feature.

    Scores every row against every candidate project, then assigns greedily from
    the highest score down so that no feature is used twice — this is what keeps
    multi-project groups (Awahuri x3, Ihaia Taueki x2) from all landing on the
    same feature.
    """
    overrides = ROW_OVERRIDES.get(year, {})

    cand = []
    for p in projects:
        cand.append({
            "globalid": norm_guid(p["GlobalID"]),
            "group": p.get("Group_name_1") or "",
            "project": p.get("ProjectNam_1") or "",
            "group_n": normalise(p.get("Group_name_1")),
            "project_n": normalise(p.get("ProjectNam_1")),
        })

    # Score every (row, candidate) pair
    scored = []
    for _, row in df.iterrows():
        applicant = row[applicant_col]
        group_part, project_part = split_applicant(applicant)

        alias = NAME_ALIASES.get(normalise(group_part))
        group_n = normalise(alias) if alias else normalise(group_part)
        project_n = normalise(project_part)

        for c in cand:
            group_score = similarity(group_n, c["group_n"])
            if project_n:
                # Weight the group name most, but let the project name break ties
                proj_score = similarity(project_n, c["project_n"])
                score = 0.65 * group_score + 0.35 * proj_score
            else:
                score = group_score
            scored.append((score, int(row["_excel_row"]), c))

    scored.sort(key=lambda t: -t[0])

    # Best and runner-up per row, for the ambiguity check
    best = {}
    runner = {}
    for score, excel_row, c in scored:
        if excel_row not in best:
            best[excel_row] = (score, c)
        elif excel_row not in runner and c["globalid"] != best[excel_row][1]["globalid"]:
            runner[excel_row] = (score, c)

    # Pin the explicit overrides first, so the greedy pass below can never take
    # a feature an override needs.
    assigned = {}
    used = set()
    by_project_n = {c["project_n"]: c for c in cand}
    for excel_row, project_name in overrides.items():
        c = by_project_n.get(normalise(project_name))
        if not c:
            log.error(f"  ROW_OVERRIDES row {excel_row}: no project named "
                      f"'{project_name}' in grant year {year}")
            continue
        assigned[excel_row] = (1.0, c)
        used.add(c["globalid"])

    # Greedy one-to-one assignment for everything else
    for score, excel_row, c in scored:
        if excel_row in assigned or c["globalid"] in used:
            continue
        if score < MATCH_ACCEPT:
            continue
        assigned[excel_row] = (score, c)
        used.add(c["globalid"])

    results = []
    for _, row in df.iterrows():
        excel_row = int(row["_excel_row"])
        hit = assigned.get(excel_row)
        top_score = best.get(excel_row, (0.0, None))[0]
        next_score = runner.get(excel_row, (0.0, None))[0]

        if excel_row in overrides and hit:
            status = "OVERRIDE"
        elif not hit:
            status = "UNMATCHED"
        elif top_score - next_score < MATCH_MARGIN:
            status = "AMBIGUOUS"
        else:
            status = "MATCHED"

        results.append({
            "excel_row": excel_row,
            "applicant": str(row[applicant_col]).strip(),
            "status": status,
            "score": round(hit[0], 3) if hit else round(top_score, 3),
            "runner_up_score": round(next_score, 3),
            "matched_group": hit[1]["group"] if hit else "",
            "matched_project": hit[1]["project"] if hit else "",
            "globalid": hit[1]["globalid"] if hit else "",
            "row": row,
        })

    unused = [c for c in cand if c["globalid"] not in used]
    return results, unused


# ============================================================
# BUILD THE STATS RECORDS
# ============================================================

def build_attributes(row, project_id, year=None, has_year_field=True):
    attrs = {"ProjectID": project_id}
    # Grant_year is what lets the dashboard charts split by financial year — the
    # stats table has no other way to tell one year's records from another.
    if year and has_year_field:
        attrs[YEAR_FIELD] = year
    for col, (field, cast) in FIELD_MAP.items():
        if col not in row.index:
            continue
        if col == "Pest Plant Control ha" and PEST_PLANT_HA_TO_M2:
            # Convert from the raw hectare value, not the cast one — rounding
            # first would turn 0.02 ha into 0 m2 instead of 200 m2.
            ha = to_float(row[col])
            attrs[field] = None if ha is None else int(round(ha * 10000))
            continue
        attrs[field] = CASTS[cast](row[col])
    return attrs


# ============================================================
# BACKFILL GRANT YEAR
# ============================================================

def backfill_years(layer, stats, has_year_field, push):
    """Stamp every existing stats row with its project's grant year.

    Run once after adding the Grant_year field in AGOL. The year is read from
    the linked project feature, so this fixes the older rows that were loaded
    before the field existed as well as any added since.
    """
    log.info("")
    log.info("BACKFILL: setting %s from each row's linked project" % YEAR_FIELD)

    if not has_year_field:
        log.error(f"Cannot backfill — '{YEAR_FIELD}' is not on the stats table yet.")
        return 1

    years = {}
    for f in layer.query(where="1=1", out_fields="GlobalID,Grant_year_1",
                         return_geometry=False).features:
        years[norm_guid(f.attributes["GlobalID"])] = f.attributes["Grant_year_1"]
    log.info(f"  {len(years)} project features read")

    rows = stats.query(where="1=1",
                       out_fields=f"OBJECTID,ProjectID,{YEAR_FIELD}").features
    log.info(f"  {len(rows)} stats rows read")

    updates, orphans, already = [], [], 0
    for r in rows:
        a = r.attributes
        year = years.get(norm_guid(a["ProjectID"]))
        if not year:
            orphans.append(a["OBJECTID"])
            continue
        if a.get(YEAR_FIELD) == year:
            already += 1
            continue
        updates.append({"attributes": {"OBJECTID": a["OBJECTID"], YEAR_FIELD: year}})

    counts = {}
    for u in updates:
        y = u["attributes"][YEAR_FIELD]
        counts[y] = counts.get(y, 0) + 1
    log.info(f"  To stamp: {len(updates)}   Already correct: {already}   "
             f"No linked project: {len(orphans)}")
    log.info(f"  By year: {dict(sorted(counts.items()))}")
    if orphans:
        log.warning(f"  Stats rows whose ProjectID matches no project (OBJECTIDs): "
                    f"{orphans}")

    if not updates:
        log.info("  Nothing to do.")
    elif not push:
        log.info("")
        log.info("DRY RUN — nothing written. Re-run with --push to apply.")
    else:
        result = stats.edit_features(updates=updates, rollback_on_failure=True)
        ok = sum(1 for x in result.get("updateResults", []) if x.get("success"))
        fails = [x for x in result.get("updateResults", []) if not x.get("success")]
        for x in fails:
            log.error(f"    FAILED: {x.get('error')}")
        if fails:
            raise RuntimeError(f"{len(fails)} update(s) failed — table unchanged.")
        log.info(f"  Stamped {ok} row(s).")

    log.info("=" * 70)
    return 0


# ============================================================
# MAIN
# ============================================================

def main():
    ap = argparse.ArgumentParser(description="Load the KKT stats spreadsheet into AGOL.")
    ap.add_argument("--year", help="Grant year to load, e.g. 25_26. "
                                   "Defaults to the year in the spreadsheet filename.")
    ap.add_argument("--xlsx", default=KKT_STATS_XLSX, help="Spreadsheet path override.")
    ap.add_argument("--push", action="store_true",
                    help="Actually write to AGOL. Without this the script only "
                         "reports what it would do.")
    ap.add_argument("--backfill-years", action="store_true",
                    help=f"Stamp every existing stats row with its project's grant "
                         f"year and exit. Run once after adding the {YEAR_FIELD} "
                         f"field in AGOL; does not read the spreadsheet.")
    ap.add_argument("--include-ambiguous", action="store_true",
                    help="Also write rows the matcher could not confidently place, "
                         "accepting its best guess. Check the review CSV first — "
                         "an ambiguous row is a coin flip between two projects.")
    args = ap.parse_args()

    year = args.year or year_from_filename(args.xlsx)
    if not year:
        log.error("Could not work out the grant year — pass --year 25_26")
        return 1

    log.info("=" * 70)
    log.info(f"KKT STATS UPDATE — grant year {year}   "
             f"({'PUSH' if args.push else 'DRY RUN'})")
    log.info("=" * 70)

    # Backfill works purely from AGOL, so don't require the spreadsheet for it.
    if not args.backfill_years:
        df, applicant_col = read_spreadsheet(args.xlsx)

    from arcgis.gis import GIS
    from arcgis.features import FeatureLayerCollection

    log.info("Connecting to AGOL using the ArcGIS Pro sign-in...")
    gis = GIS("home")
    flc = FeatureLayerCollection(KKT_SERVICE_URL, gis=gis)
    layer = next(l for l in flc.layers if l.properties.id == KKT_LAYER_ID)
    stats = next(t for t in flc.tables if t.properties.id == KKT_STATS_ID)
    log.info(f"  Layer: {layer.properties.name}   Table: {stats.properties.name}")

    has_year_field = any(f["name"] == YEAR_FIELD for f in stats.properties.fields)
    if has_year_field:
        log.info(f"  '{YEAR_FIELD}' field present — stats rows will be stamped "
                 f"with their grant year.")
    else:
        log.warning(f"  '{YEAR_FIELD}' field NOT on the stats table. Rows will be "
                    f"written without a year, and the dashboard charts will not be "
                    f"able to split by financial year. Add a text(5) field named "
                    f"'{YEAR_FIELD}' in AGOL, then re-run with --backfill-years.")

    if args.backfill_years:
        return backfill_years(layer, stats, has_year_field, args.push)

    projects = [f.attributes for f in layer.query(
        where=f"Grant_year_1 = '{year}'",
        out_fields="GlobalID,Group_name_1,ProjectNam_1,Grant_year_1",
        return_geometry=False,
    ).features]
    log.info(f"  {len(projects)} project features in grant year {year}")
    if not projects:
        log.error(f"No projects found for grant year {year} — check the --year value.")
        return 1

    results, unused = match_rows(df, applicant_col, projects, year)

    counts = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    log.info(f"  Match results: {counts}")

    for r in results:
        if r["status"] in ("UNMATCHED", "AMBIGUOUS"):
            log.warning(f"  [{r['status']}] row {r['excel_row']}: '{r['applicant']}' "
                        f"-> '{r['matched_group']} | {r['matched_project']}' "
                        f"(score {r['score']}, runner-up {r['runner_up_score']})")
    for c in unused:
        log.warning(f"  [NO SPREADSHEET ROW] {c['group']} | {c['project']}")

    # ── Existing stats rows, so we update rather than duplicate ──────────────
    existing = {}
    for f in stats.query(where="ProjectID IS NOT NULL",
                         out_fields="OBJECTID,ProjectID").features:
        existing[norm_guid(f.attributes["ProjectID"])] = f.attributes["OBJECTID"]
    log.info(f"  {len(existing)} existing stats rows in the table")

    # Anything the script isn't confident about is left alone for manual entry —
    # a wrong link here would attach a group's figures to the wrong project.
    # --include-ambiguous accepts the matcher's best guess for the flagged rows.
    # UNMATCHED is always skipped: there is no project to attach those to.
    SKIP_STATUSES = {"UNMATCHED"} if args.include_ambiguous else {"UNMATCHED", "AMBIGUOUS"}
    if args.include_ambiguous and counts.get("AMBIGUOUS"):
        log.warning(f"  --include-ambiguous: writing {counts['AMBIGUOUS']} ambiguous "
                    f"row(s) using the matcher's best guess.")

    adds, updates = [], []
    seen = {}
    for r in results:
        if r["status"] in SKIP_STATUSES:
            continue
        # Two spreadsheet rows must never land on the same project — that would
        # silently overwrite one with the other.
        if r["globalid"] in seen:
            log.error(f"  Row {r['excel_row']} ('{r['applicant']}') and row "
                      f"{seen[r['globalid']]} both map to "
                      f"'{r['matched_project']}' — fix with ROW_OVERRIDES.")
            r["status"] = "DUPLICATE"
            counts["DUPLICATE"] = counts.get("DUPLICATE", 0) + 1
            continue
        seen[r["globalid"]] = r["excel_row"]

        attrs = build_attributes(r["row"], r["globalid"], year, has_year_field)
        if r["globalid"] in existing:
            attrs["OBJECTID"] = existing[r["globalid"]]
            updates.append({"attributes": attrs})
        else:
            adds.append({"attributes": attrs})

    skipped = [r for r in results if r["status"] in SKIP_STATUSES or r["status"] == "DUPLICATE"]
    log.info(f"  To add: {len(adds)}   To update: {len(updates)}   "
             f"Left for manual entry: {len(skipped)}")
    for r in skipped:
        log.warning(f"    MANUAL: row {r['excel_row']} '{r['applicant']}' ({r['status']})")

    # ── Review CSV ───────────────────────────────────────────────────────────
    os.makedirs(KKT_OUTPUT_DIR, exist_ok=True)
    review_csv = os.path.join(KKT_OUTPUT_DIR, f"KKT_stats_match_review_{year}.csv")
    stat_fields = [f for _, (f, _) in FIELD_MAP.items()]
    with open(review_csv, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(["excel_row", "status", "score", "runner_up_score", "applicant",
                    "matched_group", "matched_project", "action", "globalid"]
                   + stat_fields)
        for r in results:
            attrs = (build_attributes(r["row"], r["globalid"], year, has_year_field)
                     if r["status"] != "UNMATCHED" else {})
            action = ("skip" if r["status"] == "UNMATCHED"
                      else "update" if r["globalid"] in existing else "add")
            w.writerow([r["excel_row"], r["status"], r["score"], r["runner_up_score"],
                        r["applicant"], r["matched_group"], r["matched_project"],
                        action, r["globalid"]]
                       + [attrs.get(f, "") for f in stat_fields])
        for c in unused:
            w.writerow(["", "NO SPREADSHEET ROW", "", "", "", c["group"], c["project"],
                        "none", c["globalid"]] + [""] * len(stat_fields))
    log.info(f"  Review CSV: {review_csv}")

    # ── Apply ────────────────────────────────────────────────────────────────
    if not args.push:
        log.info("")
        log.info("DRY RUN — nothing was written to AGOL.")
        log.info("Check the review CSV, fix any UNMATCHED/AMBIGUOUS rows via "
                 "NAME_ALIASES or ROW_OVERRIDES, then re-run with --push.")
    else:
        log.info("")
        log.info(f"Writing to {stats.properties.name}: "
                 f"{len(adds)} add(s), {len(updates)} update(s)...")
        result = stats.edit_features(adds=adds or None, updates=updates or None,
                                     rollback_on_failure=True)
        added = sum(1 for x in result.get("addResults", []) if x.get("success"))
        updated = sum(1 for x in result.get("updateResults", []) if x.get("success"))
        failures = [x for x in result.get("addResults", []) + result.get("updateResults", [])
                    if not x.get("success")]
        if failures:
            for x in failures:
                log.error(f"    FAILED: {x.get('error')}")
            raise RuntimeError(f"{len(failures)} edit(s) failed — table unchanged "
                               f"(rollback_on_failure was on).")
        log.info(f"  Added {added}, updated {updated}. Done.")
        if skipped:
            log.info(f"  {len(skipped)} row(s) were not written and need adding "
                     f"by hand — see the MANUAL lines above and the review CSV.")

    log.info("=" * 70)
    log.info(f"Log: {log_file}")
    log.info("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
