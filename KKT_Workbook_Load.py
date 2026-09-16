"""
KKT_Workbook_Load.py
====================

Loads the returned KKT data entry workbook back onto the feature service -
KKT_Projects_Layer and its related KKT_Related_Table_Statistics table.

Input is the file KKT_Workbook_Export.py generated, with the team's edits in
it. OBJECTID is the key on both halves; nothing is ever matched on name.

Dry run by default. It reports every changed cell and writes an outcome CSV
before anything is sent, because the whole point of the workbook is that a
wrong value should be visible before it reaches the dashboard, not after.

What a row means
----------------
One worksheet row is one grant - a project in one grant year - with that
year's statistics on the same row.

    OBJECTID set, statistics changed      update both records
    OBJECTID set, Stats OBJECTID blank
       and statistics filled in           update the project, CREATE its
                                          statistics record
    OBJECTID blank                        CREATE the project, then its
                                          statistics if any were entered
    row untouched                         nothing sent

Deleting a row from the workbook does nothing. Deletions stay a deliberate act
in AGOL.

Geometry for a new project, in order
------------------------------------
1.  Latitude and Longitude filled in     the point is built from them
2.  Project ID matches an existing one   the point is copied from that
                                         project's most recent grant year
3.  neither                              the row is held and written to
                                         KKT_new_projects_need_geometry.csv,
                                         to be placed by hand in Map Viewer

Nothing is ever half-written: a row that cannot be resolved is skipped whole.

Run order
---------
1.  Dry run (default) - reports what would change, writes the outcome CSV:
        python KKT_Workbook_Load.py
2.  Read the outcome CSV and the warnings.
3.  Apply:
        python KKT_Workbook_Load.py --push

Run from the ArcGIS Pro Python environment, signed in to the portal in Pro.
"""

import os
import re
import csv
import sys
import glob
import logging
import argparse
from datetime import datetime as dt

REPO_DIR = os.path.dirname(os.path.abspath(__file__))
if REPO_DIR not in sys.path:
    sys.path.insert(0, REPO_DIR)

from config import KKT_SERVICE_URL, KKT_OUTPUT_DIR

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

KKT_LAYER_ID = 4
KKT_STATS_ID = 5

# Must match KKT_Workbook_Export.py - the workbook's own shape.
HEADER_BAND_ROW = 1
HEADER_FIELD_ROW = 3
FIRST_DATA_ROW = 4
SHEET_NAME = "Grants"
KEY_STATS_OID = "Stats_OBJECTID"

# Statistics fields the workbook does not show because the loader derives them.
# ProjectID is the link to the parent project; Grant_year mirrors the project's
# Grant_year; Applicant is a free-text copy of the group name that drifts
# ("Eco School" / "The Eco School") and is set from Group_name on new rows.
DERIVED_PARENT = "ProjectID"
DERIVED_YEAR = "Grant_year"
DERIVED_APPLICANT = "Applicant"

# Statistics fields the loader fills itself, so they are never workbook columns.
# Gathered as a set because assign_bands needs it: these names may also exist on
# the projects layer without being ambiguous.
DERIVED_STATS = {DERIVED_PARENT, DERIVED_YEAR, DERIVED_APPLICANT}

# Stored as text but holding money. Read as a number, written back in the
# layer's own '$1,234.00' shape - every consumer already parses that, and a
# bare number string would be a silent change of shape.
TEXT_MONEY = {"Current_YearFund"}

# A new project is meaningless without these.
REQUIRED_NEW = ["Grant_year", "Group_name"]

GRANT_YEAR_RE = re.compile(r"^\d{2}_\d{2}$")

# The layer is NZTM2000. Latitude and longitude are entered in WGS84 and
# projected on the way in. Rough bounds for the Horizons region, used to catch
# the classic swap of the two columns.
LAYER_WKID = 2193
LATLON_WKID = 4326
NZ_LAT = (-48.0, -34.0)
NZ_LON = (166.0, 179.5)

BATCH = 200

LOG_DIR = os.path.join(REPO_DIR, "logs", "kkt")
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, dt.now().strftime('%Y-%m-%d_%H-%M-%S') + '_workbook_load.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.FileHandler(log_file, encoding='utf-8'),
              logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------- helpers --

def norm_guid(guid):
    """The layer returns GlobalIDs bare and lowercase; the stats table stores
    them braced and uppercase. Without normalising both sides, existing rows
    look unmatched and get duplicated instead of updated."""
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


def fmt_money(value):
    """Back into the layer's text shape."""
    return None if value is None else f"${value:,.2f}"


def norm_name(value):
    """Loose form of a name, for spotting a row that is already on the layer.
    Not used for matching anything to anything - OBJECTID does that."""
    return re.sub(r"\s+", " ", (to_text(value) or "")).strip().lower()


def to_text(value):
    """Blank-safe text. A whole float becomes '650', not '650.0' - openpyxl
    hands back numbers for cells that look numeric, and the layer's text
    fields would otherwise gain a decimal point they never had."""
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    s = str(value).strip()
    return s or None


# One number, optionally wrapped in words or symbols. Deliberately refuses a
# cell holding two numbers.
_NUM_RE = re.compile(r"^[^\d\-+]*([-+]?\d[\d,]*(?:\.\d+)?)[^\d]*$")


def read_number(value, issues, where):
    """A number out of a spreadsheet cell.

    A clean number passes straight through. A number wrapped in words - '650
    plants', '~40', 'about 40' - is read, and the reading is written to the
    log, because interpreting a cell is not the same as reading it and someone
    should be able to check it.

    Anything without one clear number in it is reported and left out rather
    than mangled. This is why it does not simply strip every non-digit: that
    turns '40-50' into 4050 and 'n/a 12' into 12, both silently.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    plain = s.replace("$", "").replace(",", "").strip()
    try:
        return float(plain)
    except ValueError:
        pass
    m = _NUM_RE.match(s)
    if not m:
        issues.append(f"{where}: {value!r} has no single clear number in it, "
                      f"so it was left out")
        return None
    got = float(m.group(1).replace(",", ""))
    issues.append(f"{where}: {value!r} read as {got:g}")
    return got


def to_int(value, issues, where):
    n = read_number(value, issues, where)
    return None if n is None else int(round(n))


def to_float(value, issues, where):
    n = read_number(value, issues, where)
    return None if n is None else round(n, 4)


def coerce(value, fdef, header, issues, where):
    """Workbook cell -> the value the field wants."""
    if header in TEXT_MONEY:
        return parse_money(value)
    ftype = fdef.get("type")
    if ftype in ("esriFieldTypeInteger", "esriFieldTypeSmallInteger"):
        return to_int(value, issues, where)
    if ftype in ("esriFieldTypeDouble", "esriFieldTypeSingle"):
        return to_float(value, issues, where)
    return to_text(value)


def live_value(attrs, fdef, header):
    """The same coercion applied to what AGOL currently holds, so the two are
    compared on equal terms."""
    v = attrs.get(header)
    if header in TEXT_MONEY:
        return parse_money(v)
    ftype = fdef.get("type")
    if ftype in ("esriFieldTypeInteger", "esriFieldTypeSmallInteger"):
        return None if v is None else int(round(float(v)))
    if ftype in ("esriFieldTypeDouble", "esriFieldTypeSingle"):
        return None if v is None else round(float(v), 4)
    return to_text(v)


def out_value(value, header):
    """The value as it is sent to AGOL."""
    return fmt_money(value) if header in TEXT_MONEY else value


def show(value, header):
    """Readable in the outcome CSV."""
    if value is None:
        return ""
    if header in TEXT_MONEY:
        return fmt_money(value)
    return str(value)


# ------------------------------------------------------------ the workbook --

ENTRY_COLUMNS = {"Latitude", "Longitude", "Notes"}

# Values the export pre-fills on every blank row. A row carrying nothing but
# these is an untouched spare, not an attempt at data entry, so it is skipped
# in silence rather than reported as a project missing its name.
#
# Empty since 16 September 2026, when the "Project" field it seeded was dropped
# from the layer. Blank rows are now caught by the all-None test in read_sheet.
# The mechanism is kept because it costs nothing and any future pre-filled
# column would otherwise start reporting spare rows as incomplete projects.
SEEDED = {}


def read_sheet(path):
    """The Grants sheet as raw rows: the field name from row 3 mapped to that
    row's cell value. Keying on the field name rather than the column position
    means hiding, widening or folding a column changes nothing.

    Which table each column belongs to is worked out later, against the live
    schema - see assign_bands. The coloured band headings are for the reader,
    and nothing here depends on their wording.
    """
    import openpyxl
    wb = openpyxl.load_workbook(path, data_only=True)
    if SHEET_NAME not in wb.sheetnames:
        raise RuntimeError(f"No '{SHEET_NAME}' sheet in {path}. Is this the "
                           f"workbook KKT_Workbook_Export.py generated?")
    ws = wb[SHEET_NAME]

    cols = []
    for i in range(1, ws.max_column + 1):
        field = ws.cell(HEADER_FIELD_ROW, i).value
        if field:
            cols.append({"index": i, "header": str(field).strip()})

    if not any(c["header"] == "OBJECTID" for c in cols):
        raise RuntimeError("No OBJECTID column found on row 3 - this does not "
                           "look like the generated workbook.")

    rows = []
    for r in range(FIRST_DATA_ROW, ws.max_row + 1):
        values = {}
        for c in cols:
            v = ws.cell(r, c["index"]).value
            if isinstance(v, str):
                v = v.strip() or None
            values[c["header"]] = v
        if all(v is None for v in values.values()):
            continue
        rows.append({"excel_row": r, "values": values})

    log.info(f"  Workbook: {path}")
    log.info(f"  Sheet '{SHEET_NAME}': {len(rows)} rows with content, "
             f"{len(cols)} columns")
    return cols, rows


def assign_bands(cols, layer_fields, stats_fields):
    """Which table each workbook column writes to, decided by looking the field
    name up in the live schema rather than by trusting a heading.

    A name on both tables would be ambiguous and is refused rather than guessed
    at, so a value can never be written to the wrong table silently.

    Grant_year is the exception, and a deliberate one. Both tables carry it: the
    projects layer since the _1 suffixes were dropped on 16 September 2026, and
    the statistics table all along. It is not ambiguous here because the
    statistics copy is DERIVED - the loader sets it from the project on the same
    row and never reads it from the workbook, so the export leaves it out of the
    sheet entirely. A Grant_year column in the workbook can therefore only be
    the project's. The same holds for ProjectID and Applicant.
    """
    bands, unknown, ambiguous = {}, [], []
    for c in cols:
        h = c["header"]
        if h in ("OBJECTID", KEY_STATS_OID):
            bands[h] = "KEY"
        elif h in ENTRY_COLUMNS:
            bands[h] = "ENTRY"
        elif h in layer_fields and h in stats_fields and h not in DERIVED_STATS:
            ambiguous.append(h)
        elif h in layer_fields:
            bands[h] = "PROJECT"
        elif h in stats_fields:
            bands[h] = "STATISTICS"
        else:
            unknown.append(h)
    return bands, unknown, ambiguous


def split_row(values, bands):
    """One raw row -> its key, project, statistics and entry parts."""
    rec = {"PROJECT": {}, "STATISTICS": {}, "ENTRY": {},
           "oid": None, "stats_oid": None}
    for header, v in values.items():
        band = bands.get(header)
        if header == "OBJECTID":
            rec["oid"] = v
        elif header == KEY_STATS_OID:
            rec["stats_oid"] = v
        elif band in ("PROJECT", "STATISTICS", "ENTRY"):
            rec[band][header] = v
    return rec


# ---------------------------------------------------------------- geometry --

def project_points(gis, points):
    """WGS84 lat/long -> the layer's NZTM2000, via the portal geometry
    service. Returns a list in the same order."""
    from arcgis.geometry import project
    out = project([{"x": lon, "y": lat} for lon, lat in points],
                  in_sr=LATLON_WKID, out_sr=LAYER_WKID, gis=gis)
    return [{"x": g["x"], "y": g["y"],
             "spatialReference": {"wkid": LAYER_WKID}} for g in out]


# -------------------------------------------------------------------- main --

def main():
    ap = argparse.ArgumentParser(
        description="Load the returned KKT data entry workbook onto AGOL.")
    ap.add_argument("--push", action="store_true",
                    help="Write to AGOL. Without this the script only reports.")
    ap.add_argument("--workbook", default=None,
                    help="Override the input workbook path. Defaults to the "
                         "most recently modified KKT_data_entry_*.xlsx.")
    ap.add_argument("--allow-duplicates", action="store_true",
                    help="Create a new project even where the same group, "
                         "grant year and project name is already on the layer. "
                         "Only for a genuine second grant - the guard is there "
                         "to stop a workbook being loaded twice.")
    args = ap.parse_args()

    log.info("=" * 70)
    log.info(f"KKT DATA ENTRY WORKBOOK - LOAD   ({'PUSH' if args.push else 'DRY RUN'})")
    log.info("=" * 70)

    path = args.workbook
    if not path:
        found = glob.glob(os.path.join(KKT_OUTPUT_DIR, "KKT_data_entry_*.xlsx"))
        if not found:
            log.error(f"No KKT_data_entry_*.xlsx in {KKT_OUTPUT_DIR}. "
                      f"Run KKT_Workbook_Export.py first.")
            return 1
        path = max(found, key=os.path.getmtime)
    if not os.path.exists(path):
        log.error(f"Workbook not found: {path}")
        return 1

    cols, rows = read_sheet(path)

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

    # Never create a field. A workbook column with nowhere to go is a mistake
    # to report, not a schema change to make - that stays a deliberate manual
    # act, as in every other KKT script.
    bands, unknown, ambiguous = assign_bands(cols, layer_fields, stats_fields)
    if unknown:
        log.error(f"Workbook columns with no field on the service: {unknown}")
        log.error("Add the field in AGOL first, or re-export the workbook. "
                  "This script never creates fields.")
        return 1
    if ambiguous:
        log.error(f"These names exist on both the layer and the statistics "
                  f"table, so there is no telling which one the workbook "
                  f"column means: {ambiguous}")
        log.error("Rename one of them in AGOL before loading.")
        return 1

    domains = {}
    for defs in (layer_fields, stats_fields):
        for name, f in defs.items():
            d = f.get("domain") or {}
            if d.get("codedValues"):
                domains[name] = {c["code"] for c in d["codedValues"]}

    log.info("Reading current values from the service...")
    live_projects = layer.query(where="1=1", out_fields="*",
                                return_geometry=True).features
    live_stats = table.query(where="1=1", out_fields="*").features
    proj_by_oid = {f.attributes["OBJECTID"]: f for f in live_projects}
    stats_by_oid = {f.attributes["OBJECTID"]: f.attributes for f in live_stats}
    log.info(f"  {len(proj_by_oid)} grant rows, {len(stats_by_oid)} statistics rows")

    # Group, year and project name of every grant already on the layer. A new
    # row matching all three is almost certainly this workbook being loaded a
    # second time - without this guard, a repeated --push silently creates a
    # duplicate project. Groups that genuinely hold two grants in one year
    # have different project names, so they do not collide.
    existing_keys = {}
    for f in live_projects:
        a = f.attributes
        key = (norm_name(a.get("Group_name")), to_text(a.get("Grant_year")),
               norm_name(a.get("Project_name")))
        existing_keys.setdefault(key, a["OBJECTID"])

    # Most recent grant year per project code, for the geometry copy.
    geom_by_code = {}
    for f in live_projects:
        code = f.attributes.get("Project_ID")
        if not code or not f.geometry:
            continue
        year = f.attributes.get("Grant_year") or ""
        if code not in geom_by_code or year > geom_by_code[code][0]:
            geom_by_code[code] = (year, f.geometry)

    proj_updates, proj_adds = [], []
    stats_updates, stats_adds = [], []
    report, need_geom, issues = [], [], []
    counts = {"project_update": 0, "project_add": 0, "stats_update": 0,
              "stats_add": 0, "unchanged": 0, "held": 0}

    def note(rec, target, action, field="", before="", after="", why=""):
        report.append({
            "excel_row": rec["excel_row"], "target": target, "action": action,
            "OBJECTID": rec["oid"], "Stats_OBJECTID": rec["stats_oid"],
            "Project_ID": rec["PROJECT"].get("Project_ID"),
            "Group_name": rec["PROJECT"].get("Group_name"),
            "Grant_year": rec["PROJECT"].get("Grant_year"),
            "field": field, "before": before, "after": after, "note": why,
        })

    def hold(rec, why):
        counts["held"] += 1
        log.warning(f"  row {rec['excel_row']} "
                    f"({rec['PROJECT'].get('Group_name')}) HELD - {why}")
        note(rec, "row", "held", why=why)

    for raw_row in rows:
        rec = split_row(raw_row["values"], bands)
        rec["excel_row"] = raw_row["excel_row"]

        if rec["oid"] is None and rec["stats_oid"] is None:
            entered = [v for band in ("PROJECT", "STATISTICS", "ENTRY")
                       for h, v in rec[band].items()
                       if v is not None and SEEDED.get(h) != v]
            if not entered:
                continue

        label = (f"row {rec['excel_row']} "
                 f"({rec['PROJECT'].get('Group_name') or 'no group name'})")
        row_issues = []

        # --- validate every value on the row before deciding anything -----
        bad = None
        for band, defs in (("PROJECT", layer_fields), ("STATISTICS", stats_fields)):
            for header, raw in rec[band].items():
                if raw is None:
                    continue
                fdef = defs[header]
                text = to_text(raw)
                if header in domains and text not in domains[header]:
                    # Show a few accepted values - for Grant_year that alone
                    # makes the underscore obvious, which is the single most
                    # common way this field gets typed wrong.
                    eg = ", ".join(sorted(domains[header])[:3])
                    bad = (f"{header} = {text!r} is not one of the values the "
                           f"field accepts (e.g. {eg})")
                    break
                limit = fdef.get("length")
                if (limit and fdef.get("type") == "esriFieldTypeString"
                        and header not in TEXT_MONEY
                        and text and len(text) > limit):
                    bad = (f"{header} is {len(text)} characters and the field "
                           f"holds {limit}")
                    break
            if bad:
                break

        year = to_text(rec["PROJECT"].get("Grant_year"))
        if not bad and year and not GRANT_YEAR_RE.match(year):
            bad = (f"Grant_year = {year!r} - it must be two digits, an "
                   f"underscore, two digits, e.g. 25_26")

        if bad:
            hold(rec, bad)
            continue

        oid = to_int(rec["oid"], row_issues, label)
        stats_oid = to_int(rec["stats_oid"], row_issues, label)
        has_stats = any(v is not None for v in rec["STATISTICS"].values())

        # ------------------------------------------------- existing project --
        if oid is not None:
            if oid not in proj_by_oid:
                hold(rec, f"OBJECTID {oid} is in the workbook but not on the "
                          f"layer - it may have been deleted since the export")
                continue

            before = proj_by_oid[oid].attributes
            attrs, changed = {"OBJECTID": oid}, []
            for header, raw in rec["PROJECT"].items():
                fdef = layer_fields[header]
                if not fdef.get("editable", True):
                    continue
                new = coerce(raw, fdef, header, row_issues, f"{label} {header}")
                old = live_value(before, fdef, header)
                if new != old:
                    attrs[header] = out_value(new, header)
                    changed.append((header, old, new))
            if changed:
                counts["project_update"] += 1
                proj_updates.append({"attributes": attrs})
                for header, old, new in changed:
                    note(rec, "project", "update", header,
                         show(old, header), show(new, header))
                log.info(f"  {label}: {len(changed)} project field(s) changed - "
                         f"{', '.join(h for h, _, _ in changed)}")

            # ---------------------------------------------- its statistics --
            if stats_oid is not None:
                if stats_oid not in stats_by_oid:
                    hold(rec, f"Stats OBJECTID {stats_oid} is not on the "
                              f"statistics table")
                    continue
                s_before = stats_by_oid[stats_oid]
                s_attrs, s_changed = {"OBJECTID": stats_oid}, []
                for header, raw in rec["STATISTICS"].items():
                    fdef = stats_fields[header]
                    if not fdef.get("editable", True):
                        continue
                    new = coerce(raw, fdef, header, row_issues, f"{label} {header}")
                    old = live_value(s_before, fdef, header)
                    if new != old:
                        s_attrs[header] = out_value(new, header)
                        s_changed.append((header, old, new))
                # Grant_year is derived - it must follow the project's year.
                if year and to_text(s_before.get(DERIVED_YEAR)) != year:
                    s_attrs[DERIVED_YEAR] = year
                    s_changed.append((DERIVED_YEAR,
                                      to_text(s_before.get(DERIVED_YEAR)), year))
                if s_changed:
                    counts["stats_update"] += 1
                    stats_updates.append({"attributes": s_attrs})
                    for header, old, new in s_changed:
                        note(rec, "statistics", "update", header,
                             show(old, header), show(new, header))
                    log.info(f"  {label}: {len(s_changed)} statistic(s) changed - "
                             f"{', '.join(h for h, _, _ in s_changed)}")
                elif not changed:
                    counts["unchanged"] += 1

            elif has_stats:
                # New statistics for an existing grant. The parent already has
                # a GlobalID, so this one can be built straight away.
                s_attrs = {}
                for header, raw in rec["STATISTICS"].items():
                    fdef = stats_fields[header]
                    v = coerce(raw, fdef, header, row_issues, f"{label} {header}")
                    if v is not None:
                        s_attrs[header] = out_value(v, header)
                s_attrs[DERIVED_PARENT] = norm_guid(before.get("GlobalID"))
                s_attrs[DERIVED_YEAR] = year
                s_attrs[DERIVED_APPLICANT] = to_text(before.get("Group_name"))
                counts["stats_add"] += 1
                stats_adds.append({"attributes": s_attrs})
                note(rec, "statistics", "add", "",
                     "", f"{len(s_attrs)} fields", "new statistics record")
                log.info(f"  {label}: new statistics record "
                         f"({len(s_attrs)} fields)")
            elif not changed:
                counts["unchanged"] += 1

        # ------------------------------------------------------ new project --
        else:
            if stats_oid is not None:
                hold(rec, "a Stats OBJECTID with no project OBJECTID - the key "
                          "columns were edited")
                continue

            blank = [f for f in REQUIRED_NEW if not to_text(rec["PROJECT"].get(f))]
            if blank:
                hold(rec, f"new project is missing {', '.join(blank)}")
                continue

            dup_key = (norm_name(rec["PROJECT"].get("Group_name")), year,
                       norm_name(rec["PROJECT"].get("Project_name")))
            if dup_key in existing_keys and not args.allow_duplicates:
                hold(rec, f"this group already has a {year} grant with this "
                          f"project name on the layer (OBJECTID "
                          f"{existing_keys[dup_key]}). If this workbook has "
                          f"already been loaded, export a fresh one. If it is "
                          f"genuinely a second grant, re-run with "
                          f"--allow-duplicates")
                continue

            attrs = {}
            for header, raw in rec["PROJECT"].items():
                fdef = layer_fields[header]
                if not fdef.get("editable", True):
                    continue
                v = coerce(raw, fdef, header, row_issues, f"{label} {header}")
                if v is not None:
                    attrs[header] = out_value(v, header)

            lat = to_float(rec["ENTRY"].get("Latitude"), row_issues, label)
            lon = to_float(rec["ENTRY"].get("Longitude"), row_issues, label)
            code = to_text(rec["PROJECT"].get("Project_ID"))
            geom, how = None, None

            if lat is not None and lon is not None:
                # Catch the classic swap of the two columns before it puts a
                # project in the Pacific.
                if not (NZ_LAT[0] <= lat <= NZ_LAT[1]
                        and NZ_LON[0] <= lon <= NZ_LON[1]):
                    hold(rec, f"latitude {lat} / longitude {lon} is outside New "
                              f"Zealand - are the two columns the right way round?")
                    continue
                geom, how = ("latlon", lat, lon), "from latitude/longitude"
            elif code and code in geom_by_code:
                geom = geom_by_code[code][1]
                how = f"copied from {code}'s {geom_by_code[code][0]} grant"
            else:
                need_geom.append({
                    "excel_row": rec["excel_row"],
                    "Group_name": rec["PROJECT"].get("Group_name"),
                    "Project_name": rec["PROJECT"].get("Project_name"),
                    "Grant_year": year,
                    "District": rec["PROJECT"].get("District"),
                    "Current_YearFund": rec["PROJECT"].get("Current_YearFund"),
                    "Notes": rec["ENTRY"].get("Notes"),
                })
                hold(rec, "a new project with no location - no latitude and "
                          "longitude, and no existing project code to copy from")
                continue

            s_attrs = None
            if has_stats:
                s_attrs = {}
                for header, raw in rec["STATISTICS"].items():
                    fdef = stats_fields[header]
                    v = coerce(raw, fdef, header, row_issues, f"{label} {header}")
                    if v is not None:
                        s_attrs[header] = out_value(v, header)
                s_attrs[DERIVED_YEAR] = year
                s_attrs[DERIVED_APPLICANT] = to_text(rec["PROJECT"].get("Group_name"))

            counts["project_add"] += 1
            proj_adds.append({"rec": rec, "attributes": attrs, "geom": geom,
                              "stats": s_attrs})
            note(rec, "project", "add", "", "",
                 f"{len(attrs)} fields", f"new project, geometry {how}")
            log.info(f"  {label}: NEW project, geometry {how}"
                     + (", with statistics" if s_attrs else ""))
            if s_attrs:
                counts["stats_add"] += 1

        issues.extend(row_issues)

    # ------------------------------------------------------------ summary --
    log.info("")
    log.info(f"  Projects   : {counts['project_update']} to update, "
             f"{counts['project_add']} to create")
    log.info(f"  Statistics : {counts['stats_update']} to update, "
             f"{counts['stats_add']} to create")
    log.info(f"  Unchanged  : {counts['unchanged']}   Held: {counts['held']}")

    if issues:
        log.warning("")
        log.warning(f"  {len(issues)} cell(s) were not plain numbers - check "
                    f"how each was read:")
        for msg in issues[:20]:
            log.warning(f"    {msg}")
        if len(issues) > 20:
            log.warning(f"    ... and {len(issues) - 20} more, all in the log")

    cols_out = ["excel_row", "target", "action", "OBJECTID", "Stats_OBJECTID",
                "Project_ID", "Group_name", "Grant_year",
                "field", "before", "after", "note"]
    out_csv = os.path.join(KKT_OUTPUT_DIR, "KKT_workbook_load_outcome.csv")
    with open(out_csv, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=cols_out, extrasaction="ignore")
        w.writeheader()
        w.writerows(report)
    log.info("")
    log.info(f"  Outcome CSV: {out_csv}  ({len(report)} changes)")

    if need_geom:
        geom_csv = os.path.join(KKT_OUTPUT_DIR, "KKT_new_projects_need_geometry.csv")
        with open(geom_csv, "w", newline="", encoding="utf-8-sig") as fh:
            w = csv.DictWriter(fh, fieldnames=list(need_geom[0].keys()))
            w.writeheader()
            w.writerows(need_geom)
        log.warning(f"  {len(need_geom)} new project(s) need a point placing by "
                    f"hand: {geom_csv}")
        log.warning("  Create them in Map Viewer, put their OBJECTIDs in the "
                    "workbook, and re-run.")

    # -------------------------------------------------------------- apply --
    total = (len(proj_updates) + len(proj_adds) + len(stats_updates)
             + len(stats_adds))
    if not args.push:
        log.info("")
        log.info("DRY RUN - nothing written to AGOL. Re-run with --push to apply.")
    elif not total:
        log.info("")
        log.info("Nothing to write - the service already matches the workbook.")
    else:
        log.info("")
        log.info("Writing to AGOL...")

        if proj_updates:
            apply_edits(layer, "updates", proj_updates, "project update")

        if proj_adds:
            # Project first, then its statistics: a statistics record needs its
            # parent's GlobalID, which does not exist until the project does.
            latlon = [(a["geom"][2], a["geom"][1]) for a in proj_adds
                      if isinstance(a["geom"], tuple)]
            projected = project_points(gis, latlon) if latlon else []
            it = iter(projected)
            feats = []
            for a in proj_adds:
                g = next(it) if isinstance(a["geom"], tuple) else a["geom"]
                feats.append({"attributes": a["attributes"], "geometry": g})

            results = apply_edits(layer, "adds", feats, "project create")
            for a, res in zip(proj_adds, results):
                gid = res.get("globalId")
                if a["stats"] is not None:
                    if not gid:
                        log.error(f"  row {a['rec']['excel_row']}: the new project "
                                  f"was created but returned no GlobalID, so its "
                                  f"statistics were not. Re-run to add them.")
                        continue
                    a["stats"][DERIVED_PARENT] = norm_guid(gid)
                    stats_adds.append({"attributes": a["stats"]})

        if stats_updates:
            apply_edits(table, "updates", stats_updates, "statistics update")
        if stats_adds:
            apply_edits(table, "adds", stats_adds, "statistics create")

        log.info("  Done.")
        log.info("")
        if counts["project_add"]:
            log.info("Next: give the new projects a project code -")
            log.info("      python agol-tools/KKT_Project_ID_Assign.py --push")
        log.info("Then rebuild the dashboard JSON, or the charts keep showing")
        log.info("the old snapshot -")
        log.info("      python KKT_Dashboard_Export.py --push")
        if counts["project_add"] or counts["stats_add"]:
            log.info("")
            log.info("This workbook is now out of date - the rows it created have")
            log.info("OBJECTIDs it does not know about. Export a fresh one before")
            log.info("sending anything out again:")
            log.info("      python KKT_Workbook_Export.py")

    log.info("=" * 70)
    log.info(f"Log: {log_file}")
    log.info("=" * 70)
    return 0


def apply_edits(target, kind, payload, what):
    """Send in batches of 200 with rollback on, raising if any row fails, so a
    partial write never happens quietly. Returns the per-row results."""
    key = {"adds": "addResults", "updates": "updateResults"}[kind]
    collected = []
    for i in range(0, len(payload), BATCH):
        batch = payload[i:i + BATCH]
        result = target.edit_features(**{kind: batch}, rollback_on_failure=True)
        rows = result.get(key, [])
        fails = [x for x in rows if not x.get("success")]
        for x in fails:
            log.error(f"    FAILED: {x.get('error')}")
        if fails:
            raise RuntimeError(f"{len(fails)} {what}(s) failed in batch "
                               f"{i // BATCH + 1} - that batch was rolled back.")
        collected.extend(rows)
        log.info(f"  {what}: batch {i // BATCH + 1}, {len(batch)} row(s)")
    return collected


if __name__ == "__main__":
    sys.exit(main())
