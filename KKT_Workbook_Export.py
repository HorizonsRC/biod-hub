"""
KKT_Workbook_Export.py
======================

Builds the KKT data entry workbook: one Excel file holding every grant row and
its end-of-year statistics, pre-filled from AGOL, for the biodiversity team to
edit and send back. KKT_Workbook_Load.py reads the returned file.

This script only reads AGOL. It never writes to the service.

Why one flat sheet
------------------
The projects layer and its statistics table are a 1:1 relationship -
KKT_Projects_Layer.GlobalID -> KKT_Related_Table_Statistics.ProjectID, one
stats row per project per grant year. So both sit on one row: project columns,
then statistics columns. 176 grant rows, 102 of which have statistics; the
other 74 simply have blank statistics cells, and filling them in is how a new
statistics record gets created.

That shape is the point of the whole exercise. The old process exported a
hand-picked subset and asked the team to re-key values, which is what put three
valid 24-25 grant amounts against the wrong projects in August 2026. Here every
current value is already in the cell, so nothing correct is ever retyped.

What the team can and cannot change
-----------------------------------
The sheet is protected. OBJECTID and Stats OBJECTID are locked because they are
the keys the loader matches on, and columns cannot be inserted or deleted
because that is what corrupts a round trip. Everything else is editable.
Dropdowns come from the layer's own coded-value domains, so a value typed into
a domained field cannot drift from what AGOL accepts.

Deleting a project is deliberately not possible from the workbook. Removing a
row does nothing; deletions stay a manual act in AGOL.

Run order
---------
1.  Build the workbook:
        python KKT_Workbook_Export.py
2.  Send Data/KKT data/KKT_data_entry_<date>.xlsx to the team.
3.  Put the returned file back in the same folder and run:
        python KKT_Workbook_Load.py                 # dry run, reports changes
        python KKT_Workbook_Load.py --push          # applies them

Run from the ArcGIS Pro Python environment, signed in to the portal in Pro.
"""

import os
import re
import csv
import sys
import logging
import argparse
from datetime import datetime as dt

REPO_DIR = os.path.dirname(os.path.abspath(__file__))
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

from config import KKT_SERVICE_URL, KKT_OUTPUT_DIR

# The Pro env's console is cp1252 and cannot print a macron - group names like
# Atihau and Ngawakahiamoe would raise mid-log without this.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

KKT_LAYER_ID = 4
KKT_STATS_ID = 5

# Blank rows at the bottom for new grants. Generous - a grant round adds ~40.
NEW_ROWS = 50

# Never shown. Shape and editor tracking are Esri's; CountAttachments is
# unreliable on this layer (it returns null on rows that do have photos), and
# Report_Link is built by KKT_Report_Link_Build.py from the published annual
# report PDFs - it is not staff input and plays no part in the spatial join.
# CreationDate_1/_2, Creator_1/_2 and so on are duplicate editor-tracking
# fields left behind by an old join.
#
# "Project" and "Prev_Act_Grant_1" are listed for deletion from the layer (16
# September 2026) and are named here as well as left out of PROJECT_ORDER. Just
# removing them from the order is not enough: a field on the service that the
# order does not mention is appended to the end of its band with a warning, so
# that a new field is never silently dropped. That safety net would keep putting
# these two back until the moment they are deleted.
EXCLUDE = {
    "Shape", "SHAPE", "Shape__Area", "Shape__Length",
    "GlobalID", "CountAttachments", "Report_Link",
    "CreationDate", "Creator", "EditDate", "Editor",
    "CreationDate_1", "Creator_1", "EditDate_1", "Editor_1",
    "CreationDate_2", "Creator_2", "EditDate_2", "Editor_2",
    "Project", "Prev_Act_Grant_1",
}

# Statistics fields the flat layout makes redundant. The loader fills all three
# itself, which is what stops them drifting:
#   ProjectID   the braced GlobalID of the parent project
#   Grant_year  mirrors the project's Grant_year on the same row
#   Applicant   free text, spelled differently year to year ("Eco School" /
#               "The Eco School", macron variants) - Group_name is the
#               canonical name and is already on the same row
STATS_DERIVED = {"ProjectID", "Grant_year", "Applicant"}

# Column order within each band. Deliberate, not schema order - the activity
# and funding fields are paired so a reader sees "Planting  $3,000" side by
# side rather than four types then four amounts.
# Two fields were dropped from the layer on 16 September 2026 and so are absent
# here: "Project", which held "KKT" on all 176 rows and told a reader nothing,
# and "Prev_Act_Grant_1", which nothing displayed and nobody could keep correct.
PROJECT_ORDER = [
    "Project_ID", "Grant_year", "Group_name", "Project_name",
    "District", "No_of_GrantYears", "Current_YearFund",
    "ProjectType", "ProjectType_fund",
    "ProjectType_2", "ProjectType_fund2",
    "ProjectType_3", "ProjectType_fund3",
    "ProjectType_4", "ProjectType_fund4",
    "Project_desc", "SiteLead", "Project_contactName",
    "Website", "Social_media", "TrapNZ_Link",
]

STATS_ORDER = [
    "Plants_grown", "Plants_planted", "No_plantsReleased",
    "Pest_Animal_Control", "Traps_funded", "Total_Project_Traps",
    "Pest_Plant_Control__stems", "Pest_Plant_Control_m2",
    "Total_Pest_Plant__area_controll",
    "Total_PROJECT_area", "Restoration_area_supported_Ha",
    "Fencing_LnMtr", "Monitoring", "Education", "Species_Translocation",
    "Community_members", "Community_hours", "Volunteers",
    "NEW_Assets_purchased__not_traps", "Extra_Info",
]

# Fields stored as text that hold money or a measurement. Shown as numbers in
# the workbook and written back in the layer's own text format, because every
# consumer already parses that shape - see the module docstring of
# KKT_Dashboard_Export.py. Changing the field types is a separate job.
TEXT_MONEY = {"Current_YearFund"}

# Column widths by field, everything else gets a default from its type.
WIDTHS = {
    "Group_name": 34, "Project_name": 38, "Project_desc": 46,
    "District": 20, "Current_YearFund": 15, "Notes": 40,
    "SiteLead": 20, "Project_contactName": 20,
    "Website": 26, "Social_media": 26, "TrapNZ_Link": 26,
    "Extra_Info": 34, "Plants_grown": 24, "Monitoring": 24,
    "Education": 20, "Species_Translocation": 22,
    "NEW_Assets_purchased__not_traps": 26, "Total_PROJECT_area": 18,
    "Pest_Plant_Control__stems": 20,
}

# Friendlier column headings than the field's AGOL alias.
ALIAS_OVERRIDE = {
    "SiteLead": "Site lead",
    "Project_desc": "Project description",
}

BAND_FILL = {
    "KEY":        "D9D9D9",   # grey  - locked
    "PROJECT":    "DCE6F1",   # blue  - the projects layer
    "STATISTICS": "E4EFD9",   # green - the statistics table
    "ENTRY":      "FDE9D9",   # amber - not layer fields
}

BAND_LABEL = {
    "KEY": "KEY - do not edit",
    "PROJECT": "PROJECT DETAILS  (KKT_Projects_Layer)",
    "STATISTICS": "END OF YEAR STATISTICS  (KKT_Related_Table_Statistics)",
    "ENTRY": "NEW PROJECTS / NOTES",
}

HEADER_BAND_ROW = 1
HEADER_ALIAS_ROW = 2
HEADER_FIELD_ROW = 3
FIRST_DATA_ROW = 4

LOG_DIR = os.path.join(REPO_DIR, "logs", "kkt")
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, dt.now().strftime('%Y-%m-%d_%H-%M-%S') + '_workbook_export.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.FileHandler(log_file, encoding='utf-8'),
              logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def norm_guid(guid):
    """The layer returns GlobalIDs bare and lowercase; the stats table stores
    them braced and uppercase. Both sides need normalising before they compare
    equal, or every existing row looks unmatched."""
    if not guid:
        return ""
    return "{" + str(guid).strip().strip("{}").upper() + "}"


def parse_money(value):
    """'$3,763.00 ' -> 3763.0. Returns None for blanks, 0.0 for a real zero."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return round(float(value), 2)
    cleaned = re.sub(r"[^0-9.\-]", "", str(value))
    if cleaned in ("", ".", "-"):
        return None
    try:
        return round(float(cleaned), 2)
    except ValueError:
        return None


def build_columns(layer_fields, stats_fields):
    """The column list, in band order. Each entry says which AGOL field it
    carries, which table it belongs to, and how it should behave in Excel.

    The list is built from the live schema rather than hardcoded, so a field
    added to the layer later turns up in the next export on its own. Anything
    present on the service but missing from PROJECT_ORDER / STATS_ORDER is
    appended at the end with a warning - a new field is never silently dropped.
    """
    cols = []

    def add(band, field, fdef=None, header=None, alias=None):
        name = header or field
        cols.append({
            "band": band,
            "field": field,
            "header": name,
            "alias": ALIAS_OVERRIDE.get(
                name, alias or (fdef.get("alias") if fdef else name)),
            "type": fdef.get("type") if fdef else None,
            "length": fdef.get("length") if fdef else None,
            "domain": [c["code"] for c in (fdef.get("domain") or {}).get("codedValues", [])]
                      if fdef else [],
        })

    add("KEY", "OBJECTID", layer_fields.get("OBJECTID"), alias="Project OBJECTID")
    add("KEY", None, header="Stats_OBJECTID", alias="Stats OBJECTID")

    def ordered(order, defs, exclude, band):
        seen = set()
        for name in order:
            if name in defs and name not in exclude:
                add(band, name, defs[name])
                seen.add(name)
            elif name not in defs:
                log.warning(f"  {name} is in the column order but not on the "
                            f"service - skipped.")
        extra = [n for n in defs
                 if n not in seen and n not in exclude and n not in order
                 and defs[n].get("type") != "esriFieldTypeOID"]
        for name in extra:
            log.warning(f"  {name} is on the service but not in the column "
                        f"order - appended at the end of the {band} band.")
            add(band, name, defs[name])

    ordered(PROJECT_ORDER, layer_fields, EXCLUDE, "PROJECT")
    ordered(STATS_ORDER, stats_fields, EXCLUDE | STATS_DERIVED, "STATISTICS")

    add("ENTRY", None, header="Latitude", alias="Latitude (new projects)")
    add("ENTRY", None, header="Longitude", alias="Longitude (new projects)")
    add("ENTRY", None, header="Notes", alias="Notes (not loaded)")

    return cols


def number_format(col):
    """Excel number format for a column, from its AGOL field type."""
    name, ftype = col["header"], col["type"]
    if name in TEXT_MONEY or name.startswith("ProjectType_fund"):
        return '"$"#,##0.00'
    if name in ("Latitude", "Longitude"):
        return "0.000000"
    if ftype in ("esriFieldTypeInteger", "esriFieldTypeSmallInteger"):
        return "#,##0"
    if ftype == "esriFieldTypeDouble":
        return "#,##0.00"
    return None


def cell_value(col, attrs):
    """What goes in the cell. Text-stored money becomes a real number so the
    team sees and types a number; the loader puts the text format back."""
    if col["field"] is None:
        return None
    v = attrs.get(col["field"])
    if col["header"] in TEXT_MONEY:
        return parse_money(v)
    if isinstance(v, str):
        v = v.strip()
        return v or None
    return v


def write_workbook(path, cols, rows, project_ids):
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, Protection
    from openpyxl.utils import get_column_letter
    from openpyxl.worksheet.datavalidation import DataValidation

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Grants"
    lists = wb.create_sheet("Lists")
    readme = wb.create_sheet("Read me", 0)

    last_row = FIRST_DATA_ROW + len(rows) + NEW_ROWS - 1
    thin = Side(style="thin", color="B0B0B0")

    # --- header rows -------------------------------------------------------
    band_spans = {}
    for i, col in enumerate(cols, start=1):
        band_spans.setdefault(col["band"], [i, i])[1] = i

    for band, (first, last) in band_spans.items():
        ws.merge_cells(start_row=HEADER_BAND_ROW, start_column=first,
                       end_row=HEADER_BAND_ROW, end_column=last)
        c = ws.cell(row=HEADER_BAND_ROW, column=first, value=BAND_LABEL[band])
        c.font = Font(bold=True, size=11, color="1F3864")
        c.fill = PatternFill("solid", fgColor=BAND_FILL[band])
        c.alignment = Alignment(horizontal="center", vertical="center")

    for i, col in enumerate(cols, start=1):
        a = ws.cell(row=HEADER_ALIAS_ROW, column=i, value=col["alias"])
        a.font = Font(bold=True, size=10)
        a.fill = PatternFill("solid", fgColor=BAND_FILL[col["band"]])
        a.alignment = Alignment(wrap_text=True, vertical="bottom")
        a.border = Border(bottom=thin)

        # The field name is the key the loader reads. Shown rather than hidden
        # so "which field is this?" is answerable without opening a script.
        f = ws.cell(row=HEADER_FIELD_ROW, column=i, value=col["header"])
        f.font = Font(size=8, italic=True, color="808080")
        f.fill = PatternFill("solid", fgColor=BAND_FILL[col["band"]])
        f.border = Border(bottom=Side(style="medium", color="808080"))

        ws.column_dimensions[get_column_letter(i)].width = WIDTHS.get(
            col["header"], 13 if col["type"] else 16)

    ws.row_dimensions[HEADER_BAND_ROW].height = 20
    ws.row_dimensions[HEADER_ALIAS_ROW].height = 42
    ws.row_dimensions[HEADER_FIELD_ROW].height = 12

    # --- data --------------------------------------------------------------
    unlocked = Protection(locked=False)
    key_fill = PatternFill("solid", fgColor="F2F2F2")

    for r, row in enumerate(rows, start=FIRST_DATA_ROW):
        for i, col in enumerate(cols, start=1):
            src = row["stats"] if col["band"] == "STATISTICS" else row["project"]
            if col["header"] == "Stats_OBJECTID":
                value = (row["stats"] or {}).get("OBJECTID")
            else:
                value = cell_value(col, src or {})
            c = ws.cell(row=r, column=i, value=value)
            fmt = number_format(col)
            if fmt:
                c.number_format = fmt
            if col["band"] == "KEY":
                c.fill = key_fill
                c.font = Font(color="808080")
            else:
                c.protection = unlocked

    # Blank rows for new grants, carrying the same formats and protection so a
    # new row behaves exactly like an existing one.
    for r in range(FIRST_DATA_ROW + len(rows), last_row + 1):
        for i, col in enumerate(cols, start=1):
            c = ws.cell(row=r, column=i)
            fmt = number_format(col)
            if fmt:
                c.number_format = fmt
            if col["band"] == "KEY":
                c.fill = key_fill
            else:
                c.protection = unlocked

    # --- dropdowns ---------------------------------------------------------
    # One Lists column per distinct value set, so the four identical
    # ProjectType domains share one list rather than repeating it four times.
    list_cols, seen_sets = {}, {}
    next_col = 1

    def list_range(values, title):
        nonlocal next_col
        key = tuple(values)
        if key in seen_sets:
            return seen_sets[key]
        letter = get_column_letter(next_col)
        lists.cell(row=1, column=next_col, value=title).font = Font(bold=True)
        for j, v in enumerate(values, start=2):
            lists.cell(row=j, column=next_col, value=v)
        ref = f"Lists!${letter}$2:${letter}${len(values) + 1}"
        seen_sets[key] = ref
        list_cols[title] = ref
        next_col += 1
        return ref

    for i, col in enumerate(cols, start=1):
        letter = get_column_letter(i)
        rng = f"{letter}{FIRST_DATA_ROW}:{letter}{last_row}"

        if col["domain"]:
            ref = list_range(col["domain"], col["header"])
            # No leading "=" - the sheet XML holds a bare reference, and Excel
            # treats "=Lists!..." as a damaged file and repairs the validation
            # away on open.
            dv = DataValidation(type="list", formula1=ref, allow_blank=True,
                                showErrorMessage=True)
            dv.error = (f"Not one of the values {col['header']} accepts. Pick from "
                        f"the dropdown - anything else is rejected by AGOL.")
            dv.errorTitle = "Value not in the list"
            ws.add_data_validation(dv)
            dv.add(rng)

        elif col["header"] == "Project_ID":
            ref = list_range(project_ids, "Project_ID")
            dv = DataValidation(type="list", formula1=ref, allow_blank=True,
                                showErrorMessage=True)
            dv.error = ("Pick an existing project code to add another grant year "
                        "for that project - its map point is copied across. Leave "
                        "blank for a project that has never been funded before.")
            dv.errorTitle = "Existing projects only"
            ws.add_data_validation(dv)
            dv.add(rng)

        elif col["type"] in ("esriFieldTypeInteger", "esriFieldTypeSmallInteger"):
            dv = DataValidation(type="whole", operator="greaterThanOrEqual",
                                formula1=0, allow_blank=True, showErrorMessage=True)
            dv.error = ("This field holds a whole number. '1,200 plants' or '~40' "
                        "cannot be stored and would be dropped on load - put the "
                        "number here and the detail in Notes.")
            dv.errorTitle = "Whole numbers only"
            ws.add_data_validation(dv)
            dv.add(rng)

    # --- usability ---------------------------------------------------------
    # Freeze through Group name, so the group and year stay on screen while
    # scrolling right through 48 columns.
    freeze_at = 1
    for i, col in enumerate(cols, start=1):
        if col["header"] == "Group_name":
            freeze_at = i + 1
            break
    ws.freeze_panes = f"{get_column_letter(freeze_at)}{FIRST_DATA_ROW}"

    # Collapsible bands: fold the statistics away while editing project
    # details, or the other way round.
    #
    # Set outline_level per column rather than calling column_dimensions.group()
    # - group() deletes the individual ColumnDimension entries and spans one
    # across the range, which wipes every width set above.
    for band in ("PROJECT", "STATISTICS"):
        first, last = band_spans[band]
        for idx in range(max(first, freeze_at), last + 1):
            ws.column_dimensions[get_column_letter(idx)].outline_level = 1
    ws.sheet_properties.outlinePr.summaryRight = False

    ws.auto_filter.ref = (f"A{HEADER_FIELD_ROW}:"
                          f"{get_column_letter(len(cols))}{last_row}")

    # --- protection --------------------------------------------------------
    # A guard rail, not security - no password. It stops the two things that
    # actually corrupt a round trip: editing the key, and inserting or deleting
    # columns. Sorting is blocked because sorting a single column would
    # decouple values from their OBJECTID; filtering only hides rows, so it
    # stays available.
    ws.protection.sheet = True
    ws.protection.formatColumns = False
    ws.protection.formatRows = False
    ws.protection.autoFilter = False
    ws.protection.sort = True
    ws.protection.insertColumns = True
    ws.protection.insertRows = True
    ws.protection.deleteColumns = True
    ws.protection.deleteRows = True
    ws.protection.selectLockedCells = False

    lists.sheet_state = "hidden"
    lists.protection.sheet = True

    # --- read me -----------------------------------------------------------
    guide = [
        ("KKT data entry workbook", True),
        (f"Generated {dt.now():%d %B %Y} from Biodiversity_KKT_Projects.", False),
        ("", False),
        ("What this is", True),
        ("One row per grant: a project in one grant year, with that year's", False),
        ("end-of-year statistics on the same row. Everything currently held in", False),
        ("AGOL is already filled in, so you only change what has changed.", False),
        ("", False),
        ("The colour bands", True),
        ("Grey    the keys. Locked - they are how your edits find their way back.", False),
        ("Blue    project details.", False),
        ("Green   end-of-year statistics.", False),
        ("Amber   only needed when adding a project that has never been funded.", False),
        ("", False),
        ("Use the - and + buttons above the column letters to fold the blue or", False),
        ("green band away while you work on the other.", False),
        ("", False),
        ("Adding this year's statistics", True),
        ("Find the project's row for this grant year and type into the green", False),
        ("columns. A blank Stats OBJECTID just means no figures have been", False),
        ("recorded yet - filling the row in is what creates them.", False),
        ("", False),
        ("Adding a grant", True),
        ("Use the blank rows at the bottom. Leave the grey key columns empty.", False),
        ("", False),
        ("If the project has been funded before, pick its code from the", False),
        ("Project ID dropdown - its map point is copied across automatically.", False),
        ("", False),
        ("If it has never been funded before, leave Project ID blank. Put a", False),
        ("latitude and longitude in the amber columns if you have them; if you", False),
        ("do not, the project is still loaded and we place the point by hand.", False),
        ("", False),
        ("Things that will not work", True),
        ("Deleting a row does not delete anything - tell us instead.", False),
        ("Inserting, deleting or reordering columns is blocked.", False),
        ("Typing a value into a dropdown field that is not on its list is", False),
        ("blocked, because AGOL would reject it.", False),
        ("", False),
        ("Anything that does not fit a column goes in Notes, on the far right.", False),
        ("Notes are read by a person, not loaded - they are the best place to", False),
        ("explain an unusual figure.", False),
    ]
    for i, (text, bold) in enumerate(guide, start=1):
        c = readme.cell(row=i, column=1, value=text)
        if bold:
            c.font = Font(bold=True, size=12 if i == 1 else 11, color="1F3864")
    readme.column_dimensions["A"].width = 80
    readme.protection.sheet = True

    wb.save(path)


def write_backup_csv(path, cols, rows):
    """Flat snapshot of both tables as exported, so there is something to
    compare against or roll back to. Same naming as the other KKT pre-push
    backups in Data/KKT data/."""
    headers = [c["header"] for c in cols]
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=headers, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            out = {}
            for col in cols:
                if col["header"] == "Stats_OBJECTID":
                    out[col["header"]] = (row["stats"] or {}).get("OBJECTID")
                elif col["field"]:
                    src = row["stats"] if col["band"] == "STATISTICS" else row["project"]
                    out[col["header"]] = (src or {}).get(col["field"])
            w.writerow(out)


def main():
    ap = argparse.ArgumentParser(
        description="Build the KKT data entry workbook from AGOL. Reads only.")
    ap.add_argument("--out", default=None,
                    help="Override the output .xlsx path.")
    args = ap.parse_args()

    stamp = dt.now().strftime("%Y-%m-%d")
    out_xlsx = args.out or os.path.join(KKT_OUTPUT_DIR,
                                        f"KKT_data_entry_{stamp}.xlsx")
    out_csv = os.path.join(KKT_OUTPUT_DIR,
                           f"KKT_workbook_PREPUSH_backup_{dt.now():%Y%m%d}.csv")

    log.info("=" * 70)
    log.info("KKT DATA ENTRY WORKBOOK - EXPORT   (reads AGOL, writes nothing to it)")
    log.info("=" * 70)

    from arcgis.gis import GIS
    from arcgis.features import FeatureLayerCollection

    log.info("Connecting to AGOL using the ArcGIS Pro sign-in...")
    gis = GIS("home")
    flc = FeatureLayerCollection(KKT_SERVICE_URL, gis=gis)
    layer = next(l for l in flc.layers if l.properties.id == KKT_LAYER_ID)
    table = next(t for t in flc.tables if t.properties.id == KKT_STATS_ID)
    log.info(f"  Layer: {layer.properties.name}")
    log.info(f"  Table: {table.properties.name}")

    layer_fields = {f["name"]: dict(f) for f in layer.properties.fields}
    stats_fields = {f["name"]: dict(f) for f in table.properties.fields}

    log.info("Building the column list from the live schema...")
    cols = build_columns(layer_fields, stats_fields)
    bands = {}
    for c in cols:
        bands[c["band"]] = bands.get(c["band"], 0) + 1
    log.info(f"  {len(cols)} columns: " +
             ", ".join(f"{n} {b.lower()}" for b, n in bands.items()))

    undomained = [c["header"] for c in cols
                  if c["band"] == "PROJECT" and not c["domain"]
                  and c["type"] == "esriFieldTypeString"
                  and c["header"] not in ("Group_name", "Project_name",
                                          "Project_desc", "Current_YearFund",
                                          "Website", "Social_media", "TrapNZ_Link",
                                          "SiteLead", "Project_contactName",
                                          "Project_ID")]
    if undomained:
        log.warning(f"  No coded-value domain, so no dropdown: {undomained}")
        log.warning("  Adding a domain in AGOL would give these one.")

    log.info("Reading the projects layer and statistics table...")
    projects = layer.query(where="1=1", out_fields="*",
                           return_geometry=False).features
    stats = table.query(where="1=1", out_fields="*").features
    log.info(f"  {len(projects)} grant rows, {len(stats)} statistics rows")

    # Join on the 1:1 relationship. Both sides normalised - the layer returns
    # GlobalIDs bare and lowercase, the stats table stores them braced and
    # uppercase, so an un-normalised compare finds nothing.
    stats_by_parent = {}
    for s in stats:
        key = norm_guid(s.attributes.get("ProjectID"))
        if key in stats_by_parent:
            log.error(f"  Statistics OID {s.attributes['OBJECTID']} is a second "
                      f"row for the same project. The workbook is built on a 1:1 "
                      f"relationship and shows only the first - fix this in AGOL.")
            continue
        stats_by_parent[key] = s.attributes

    rows = []
    for p in sorted(projects, key=lambda f: (
            f.attributes.get("Grant_year") or "",
            (f.attributes.get("Group_name") or "").lower())):
        key = norm_guid(p.attributes.get("GlobalID"))
        rows.append({"project": p.attributes, "stats": stats_by_parent.pop(key, None)})
    orphans = list(stats_by_parent.values())

    if orphans:
        log.error(f"  {len(orphans)} statistics row(s) point at no project and are "
                  f"not in the workbook: "
                  f"{[o['OBJECTID'] for o in orphans]}")

    with_stats = sum(1 for r in rows if r["stats"])
    log.info(f"  {with_stats} rows carry statistics, "
             f"{len(rows) - with_stats} are blank and ready for them")

    project_ids = sorted({p.attributes.get("Project_ID") for p in projects
                          if p.attributes.get("Project_ID")})
    log.info(f"  {len(project_ids)} distinct project codes for the dropdown")

    os.makedirs(KKT_OUTPUT_DIR, exist_ok=True)
    log.info("Writing the workbook...")

    # Excel takes an exclusive lock on an open file, and the same export path is
    # re-used every day, so re-running while the previous workbook is still open
    # is the ordinary case rather than the unusual one. Say so plainly instead
    # of letting a PermissionError traceback out.
    try:
        write_workbook(out_xlsx, cols, rows, project_ids)
    except PermissionError:
        log.error(f"Could not write {out_xlsx}")
        log.error("It looks like that file is open in Excel. Close it and run "
                  "this again - nothing has been changed.")
        return 1
    log.info(f"  Workbook: {out_xlsx}")
    log.info(f"  {len(rows)} grant rows plus {NEW_ROWS} blank rows for new grants")

    try:
        write_backup_csv(out_csv, cols, rows)
    except PermissionError:
        log.error(f"Could not write the backup {out_csv} - it is open in "
                  "another program. The workbook was written; close the CSV "
                  "and re-run to refresh the backup.")
        return 1
    log.info(f"  Backup:   {out_csv}")

    log.info("")
    log.info("Next: send the workbook to the team. When it comes back, put it in")
    log.info(f"      {KKT_OUTPUT_DIR} and run -")
    log.info("      python KKT_Workbook_Load.py            # dry run")
    log.info("      python KKT_Workbook_Load.py --push     # apply")
    log.info("=" * 70)
    log.info(f"Log: {log_file}")
    log.info("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
