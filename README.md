# biod-hub

Repository for the Biodiversity Programme Hub — HTML dashboard content embedded in AGOL Experience Builder, and the Python automation scripts that process and publish biodiversity programme data.

## Repository structure

```
biod-hub/
├── html/
│   ├── index.html                               # Main hub landing page — loads hub_stats.json
│   ├── hub_stats.json                           # Auto-updated by Hub_Stats_Export.py
│   ├── pressure-management/
│   │   ├── pressure-management-dashboard.html   # Scoring criteria page (Scoring tab)
│   │   ├── PH_Pressure_Scores_Overview.html     # Chart — loads dashboard_data.json at runtime
│   │   ├── PH_Pressure_Scores_by_District.html  # Chart — loads dashboard_data.json at runtime
│   │   ├── PH_Single_Pressure_Overview.html     # Chart — loads dashboard_data.json at runtime
│   │   ├── PH_Pressure_by_Ecosystem.html        # Chart — loads dashboard_data.json at runtime
│   │   ├── PH_Sites_Threshold_Status.html       # Chart — loads dashboard_data.json at runtime
│   │   ├── PH_Sites_YoY_Threshold.html          # Chart — loads dashboard_data.json at runtime
│   │   ├── PH_Average_Score_by_Year.html        # Chart — average pressure score by financial year
│   │   ├── PH_Site_Pressure_Summary.html        # Per-site pressure card (?site= URL param)
│   │   ├── PH_Pressure_Icons.html               # Static pressure-type image panel
│   │   ├── PM_Legend.html                       # Shared chart legend, its own embed in the app
│   │   └── dashboard_data.json                  # Auto-updated by PM_Dashboard_Export.py
│   ├── kkt/
│   │   ├── KKT-dashboard.html                   # Overview panel — loads dashboard_data.json
│   │   ├── KKT_Project_Stats.html               # Chart — loads dashboard_data.json (?metric=, ?year=)
│   │   ├── KKT_Projects_by_Type.html            # Chart — loads dashboard_data.json
│   │   ├── KKT_Legend.html                      # Shared chart legend, its own embed in the app
│   │   ├── KKT_Popup_Preview.html               # Local preview of the web map pop-up layout
│   │   ├── KKT_Popup_Arcade.txt                 # Arcade expression behind the web map pop-up
│   │   └── dashboard_data.json                  # Auto-updated by KKT_Dashboard_Export.py
│   ├── targeted-rates/
│   │   ├── Targeted-rate.html                   # Cover page — one card per project
│   │   ├── WBP.html                             # Auto-updated by Targeted_Rates_Data_Export.py
│   │   └── REG.html                             # Auto-updated by Targeted_Rates_Data_Export.py
│   ├── icon-sites/
│   │   ├── icon-sites.html                      # Cover page — cards linking to each site page
│   │   ├── header.html                          # Title strip above the site pages
│   │   ├── bushy-park.html                      # Static — no automated feed yet
│   │   ├── te-apiti.html                        # Auto-updated by Icon_Sites_Data_Export.py
│   │   ├── kia-wharite.html                     # Auto-updated by Icon_Sites_Data_Export.py (PCO RTCI from local GDB)
│   │   ├── manawatu-estuary.html                # Auto-updated by Icon_Sites_Data_Export.py
│   │   ├── pukaha.html                          # Auto-updated by Icon_Sites_Data_Export.py
│   │   └── ruahine-kiwi.html                    # Auto-updated by Icon_Sites_Data_Export.py (Trap.NZ + local GDB)
│   └── totara-reserve/
│       ├── totara-reserve.html                  # Cover page — cards linking to each programme page
│       ├── pest-animal-header.html              # Pest Animal title strip + 5 tiles — auto-updated
│       ├── predator-control.html                # Pest Animal main page — auto-updated
│       ├── pest-plant-header.html               # Pest Plant title strip + 5 tiles — auto-updated
│       ├── pest-plant-control.html              # Pest Plant charts panel — auto-updated
│       ├── pest-plant-legend.html               # Pest Plant map legend — auto-updated
│       ├── biodiversity.html                    # Species records, bird counts and bats — auto-updated
│       ├── campground.html                      # Campground occupancy from Newbook — auto-updated
│       ├── river-management-header.html         # River title strip + LAWA link (static)
│       ├── river-management.html                # River levels and swim quality (not embedded in the app)
│       └── fit.js                               # Scales a page as one piece to fit its iframe
├── Run_All_Updates.py                           # Master runner — runs every script below in order
├── Run_All_Updates.bat                          # Wrapper for the Windows scheduled task
├── KKT_Stats_Update.py                          # Loads the KKT stats spreadsheet into AGOL
├── KKT_Workbook_Export.py                       # Builds the team's KKT data entry workbook
├── KKT_Workbook_Load.py                         # Diffs the edited workbook back into AGOL
├── KKT_Dashboard_Export.py                      # Builds html/kkt/dashboard_data.json
├── Pressure_Management_Data_Join.py             # Pressure Management data pipeline
├── PM_Dashboard_Export.py                       # Builds dashboard_data.json and pushes to GitHub
├── Icon_Sites_Data_Export.py                    # Queries AGOL, updates icon site HTML dashboards
├── Icon_Sites_Report_Check.py                   # Checks page figures against the annual reports
├── Targeted_Rates_Data_Export.py                # Queries AGOL, updates the WBP and REG pages
├── Totara_Reserve_Data_Export.py                # Updates the Tōtara Reserve pages (see below)
├── Hub_Stats_Export.py                          # Builds html/hub_stats.json (landing page figures)
├── config.py                                    # Local paths and item IDs — gitignored, not committed
├── config.example.py                            # Template for config.py
└── requirements.txt
```

HTML files are served via GitHub Pages at `https://HorizonsRC.github.io/biod-hub/`.

## Programmes

| Programme | Dashboard files | Updated by |
|---|---|---|
| Priority Habitats Pressure Management | `html/pressure-management/` | `Pressure_Management_Data_Join.py` → `PM_Dashboard_Export.py` |
| KKT Fund | `html/kkt/` | `KKT_Dashboard_Export.py` |
| Targeted Rates | `html/targeted-rates/` | `Targeted_Rates_Data_Export.py` |
| Icon Sites | `html/icon-sites/` | `Icon_Sites_Data_Export.py` |
| Tōtara Reserve | `html/totara-reserve/` | `Totara_Reserve_Data_Export.py` |

All five are built and embedded in their Experience Builder apps.

## Running every update in one go

`Run_All_Updates.py` runs each script below in order, one per subprocess so a failure
does not stop the rest, and commits and pushes the pages for the scripts that do not
push for themselves. Every script's output goes to its own file under
`logs/runner/<timestamp>/`, and the runner exits non-zero if any step failed, so a
Windows scheduled task records the run as failed.

```
python Run_All_Updates.py                 # everything enabled in STEPS
python Run_All_Updates.py --dry-run       # list the steps, run nothing
python Run_All_Updates.py --only hub      # one step by name
```

`Run_All_Updates.bat` is the wrapper the scheduled task calls: it names the ArcGIS Pro
conda `python.exe` explicitly (Task Scheduler cannot pick the environment on its own),
sets `PYTHONIOENCODING=utf-8` and passes any arguments straight through.

| Step | Script | Notes |
|---|---|---|
| `pm-join` | `Pressure_Management_Data_Join.py` | Only changes anything after a new SharePoint CSV export; also pushes to AGOL |
| `pm-export` | `PM_Dashboard_Export.py` | Skipped if `pm-join` failed; pushes for itself |
| `icon-sites` | `Icon_Sites_Data_Export.py` | Pushes for itself |
| `kkt` | `KKT_Dashboard_Export.py` | **Disabled** in a normal run — the KKT data changes once a year. `--only kkt` still runs it |
| `hub` | `Hub_Stats_Export.py` | Pushes for itself |
| `targeted-rates` | `Targeted_Rates_Data_Export.py` | Runner pushes `WBP.html` and `REG.html` |
| `totara-pco` | `Totara_Reserve_Data_Export.py --only pco` | Runner pushes the two pest animal pages |
| `totara-plants` | `… --only plants` | Runner pushes the three pest plant pages |
| `totara-bio` | `… --only bio` | Runner pushes `biodiversity.html` |
| `totara-camp` | `… --only camp` | Runner pushes `campground.html` |

The Tōtara `--only river` section is left out: `river-management.html` is not embedded in
the app.

The runner pulls with `--rebase --autostash` before anything else, because pages are
sometimes edited directly on github.com and every push would otherwise be rejected.

### Logging

`logs/runner/<timestamp>/runner.log` holds the outcome of each step, with one log file
per step beside it. `logs/runner/wrapper.log` records each scheduled-task invocation
and its exit code (`0` fine, `1` a step failed, `2` the Pro python was not found).

## Hub landing page stats

`Hub_Stats_Export.py` builds `html/hub_stats.json`, which `index.html` fetches at runtime to fill the "Programme at a Glance" strip and the header's reporting-period badge. Same arrangement as the other dashboards — the HTML is static and never regenerated.

```bash
python Hub_Stats_Export.py            # write the JSON
python Hub_Stats_Export.py --push     # write, commit and push to GitHub Pages
```

| Figure | Source |
|---|---|
| Priority Habitat sites | `Priority_habitats_with_PCO` row count — every site in the actively managed priority habitats layer. Deliberately not the pressure management dataset, which reclassifies four of these sites as Icon Sites or the Regional Park and so reports fewer |
| KKT grants | `Biodiversity_KKT_Projects` layer 4 row count — grants awarded across all years, not distinct projects |
| Reporting period | The financial year just finished, derived from today's date (NZ FYs run 1 July – 30 June) |

The figures currently in the HTML are fallbacks: if the JSON fails to load, the page shows them rather than rendering blank. They go stale, so run this script whenever the underlying data changes — after sites are added to the priority habitats layer or `KKT_Stats_Update.py` runs, and at the start of each financial year.

### Logging

Logs go to `logs/hub/`.

## Icon Sites script

`Icon_Sites_Data_Export.py` queries two AGOL feature layers — the BioD Contractor Data layer (pest plant control records) and the Animal Pest Control layer (trap network and inspection records) — processes the data per icon site, and injects the results directly into the corresponding HTML dashboard as a `const DATA = {...}` block. The updated HTML is then committed and pushed to GitHub Pages automatically.

### Data sources

| Source | Content | Config key |
|---|---|---|
| BioD Contractor Data feature layer (AGOL) | Waypoints (weed locations) and polylines (track coverage) | `CONTRACTOR_ITEM_ID` |
| Animal Pest Control Layer New (AGOL FeatureServer) | Trap network features and related inspection/catch records | `TRAP_SERVICE_URL` |
| Kia Whārite Project GDB (local network path) | PCO treatment area polygons with RTCI results | `KIA_WHARITE_GDB` |
| PCO Management Dataset (AGOL FeatureServer) | Horizons PCO zone RTCI monitoring results — used for RTCI pills | `PCO_MONITORING_URL` |
| HRC Icon Sites Projects layer (AGOL FeatureServer) | Reserve and buffer zone polygon areas | `HRC_ICON_SITES_URL` |
| Trap.NZ public killcount endpoint | Ruahine Kiwi Project catch totals by species and period (no auth required) | `TRAPNZ_RUAHINE_URL` |
| Ruahine Kiwi traps GDB (local network path) | Trap locations with type attributes — used for trap type breakdown chart | `RUAHINE_TRAPS_LAYER` |

### Per-site outputs

| Site | HTML file | Data source | Notes |
|---|---|---|---|
| Te Āpiti – Manawatū Gorge | `html/icon-sites/te-apiti.html` | AGOL (BioD Contractor Data + Animal Pest Control) | Pest plant and trap catch data; all-years summary CSV written to `ICON_SITES_OUTPUT_DIR` |
| Kia Whārite | `html/icon-sites/kia-wharite.html` | Local GDB (`KIA_WHARITE_GDB`) | PCO RTCI values from `PCO_Treatment_Area_ExportFeatures`; trap catch and weed data are static (from annual reports) |
| Manawatū Estuary | `html/icon-sites/manawatu-estuary.html` | AGOL (BioD Contractor Data + Animal Pest Control) | Weed count + area by species (FY 23-24 and 24-25); trap catches by species; SiteID `Horo34W` |
| Pukaha | `html/icon-sites/pukaha.html` | AGOL (PCO Management Dataset + HRC Icon Sites Projects) | OMB pest control and monitoring metrics; RTCI pills per PCO zone; reserve/buffer polygon areas from HRC Icon Sites Projects layer |
| Ruahine Kiwi | `html/icon-sites/ruahine-kiwi.html` | Trap.NZ public API + Local GDB + AGOL (PCO Management Dataset) | Catch totals and species breakdown from Trap.NZ; trap type chart from local GDB; Horizons PCO RTCI pills for adjacent zones |

### Setup

1. Copy `config.example.py` to `config.py` and fill in the required keys:
   - `CONTRACTOR_ITEM_ID` — AGOL item ID for the BioD Contractor Data feature layer
   - `TRAP_SERVICE_URL` — FeatureServer URL for the Animal Pest Control layer
   - `KIA_WHARITE_GDB` — network path to the Kia Whārite Project File Geodatabase
   - `PCO_MONITORING_URL` — FeatureServer URL for the PCO Management Dataset (RTCI results)
   - `HRC_ICON_SITES_URL` — FeatureServer URL for the HRC Icon Sites Projects layer
   - `TRAPNZ_RUAHINE_URL` — Trap.NZ public killcount endpoint for the Ruahine Kiwi Project
   - `RUAHINE_TRAPS_LAYER` — network path to the Ruahine Kiwi trap locations GDB feature class
   - `ICON_SITES_OUTPUT_DIR` — local folder for per-site summary CSVs (gitignored)
2. Run from the ArcGIS Pro Python environment (`arcgispro-py3`):

```
C:\Users\<you>\AppData\Local\ESRI\conda\envs\arcpro-scripts-3-5\python.exe Icon_Sites_Data_Export.py
```

3. Ensure you are signed in to ArcGIS Pro with your portal credentials before running

### Adding a new icon site

1. Create the HTML file in `html/icon-sites/` with the marker comments:
   ```
   /* ICON_SITE_DATA_START — ... */
   const DATA = { ... };
   /* ICON_SITE_DATA_END */
   ```
2. Add the site key and HTML path to `ICON_SITE_HTML` in `Icon_Sites_Data_Export.py`
3. Write a `process_<site>()` function and call it in `main()`

### Logging

Log files are written to `logs/icon-sites/` (gitignored).

---

## KKT scripts

`KKT_Stats_Update.py` loads the team's annual *Summary KKT stats* spreadsheet into the `KKT_Related_Table_Statistics` table on the `Biodiversity_KKT_Projects` service, matching each spreadsheet row to its project feature in `KKT_Projects_Layer` for a given grant year.

The layer/table relationship is 1:1 — `KKT_Projects_Layer.GlobalID` → `KKT_Related_Table_Statistics.ProjectID`. Each layer feature is one project *for one grant year*, so one stats row per feature is correct.

### How it runs

The script is a dry run by default — it matches names, writes a review CSV and changes nothing:

```
python KKT_Stats_Update.py --year 25_26
```

Check the review CSV, then apply it:

```
python KKT_Stats_Update.py --year 25_26 --push
```

Rows whose project already has a stats row are **updated** (matched on `ProjectID`), so re-running does not create duplicates. Rows the script is not confident about are left alone for manual entry rather than guessed at.

### Name matching

The spreadsheet's `Applicant` column is free text and does not match the layer cleanly — macron differences, missing suffixes ("Kopua Bush" vs "Kopua Bush Remnant Management Group"), and groups that run several projects in one year. Matching works in three layers:

| Mechanism | Purpose |
|---|---|
| Fuzzy match | Normalises both names (lowercase, macrons stripped, punctuation and filler words removed) and scores them. Assignment is one-to-one so two rows never land on the same project. |
| `NAME_ALIASES` | Spreadsheet group name → `Group_name`, for groups the fuzzy match cannot reach. |
| `ROW_OVERRIDES` | Spreadsheet row number → exact `Project_name`, for groups with several projects that the sheet gives no project name for. Keyed by grant year. |

Rows reported as `UNMATCHED` or `AMBIGUOUS` are skipped and listed in the log as `MANUAL:` lines — resolve them by adding an alias or override, or enter them by hand in AGOL.

### Field mapping notes

`FIELD_MAP` at the top of the script maps each spreadsheet column to a table field. Two mappings are deliberate decisions rather than obvious ones:

- **`Pest Plant Control ha` → `Pest_Plant_Control_m2`** — the sheet is in hectares but the field is m², so values are multiplied by 10,000. Set `PEST_PLANT_HA_TO_M2 = False` to store raw hectares instead.
- **`# Plants funded` → `Plants_planted`** — treated as the same figure.

The `Catchment` column has no matching field in the table and is ignored. New columns added to the spreadsheet are logged as a warning rather than silently dropped.

### Setup

Add to `config.py`:
- `KKT_SERVICE_URL` — FeatureServer URL for `Biodiversity_KKT_Projects` (layer 4 = projects, table 5 = statistics)
- `KKT_STATS_XLSX` — path to the team's summary statistics spreadsheet
- `KKT_OUTPUT_DIR` — folder for the match review CSV (gitignored)

### Grant year on the stats table

`KKT_Related_Table_Statistics` has a `Grant_year` field (text, 5 — e.g. `25_26`) mirroring `Grant_year` on the projects layer. The table has no other year marker, so without it nothing can split the stats by financial year. `KKT_Stats_Update.py` stamps it on every row it writes; to fix rows loaded before the field existed:

```
python KKT_Stats_Update.py --backfill-years          # dry run
python KKT_Stats_Update.py --backfill-years --push   # apply
```

The backfill reads each row's year from its linked project, so it works for every year at once and does not need the spreadsheet.

### Dashboard charts

`KKT_Dashboard_Export.py` queries the projects layer and stats table and writes `html/kkt/dashboard_data.json`, which the chart pages fetch at runtime — the same arrangement as Pressure Management, so the HTML is static and never regenerated.

```
python KKT_Dashboard_Export.py            # write the JSON
python KKT_Dashboard_Export.py --push     # write, commit and push to GitHub Pages
```

The export joins each stats row to its project and adds `group_name`, `project_name` and `district`. Charts group by `group_name` (the canonical `Group_name`) rather than the stats table's `Applicant`, which is free text copied from the spreadsheet and spells the same group differently year to year — grouping by `Applicant` splits one group into several bars. It also parses `Current_YearFund` (`'$3,763.00 '`) into a numeric `funding` field.

| Page | Replaces | URL parameters |
|---|---|---|
| `KKT_Project_Stats.html` | The four ExB chart widgets (Community Hours, Member Numbers, Pest Control, Planting) | `?metric=` one of `plants`, `traps`, `hours`, `members`, `animals`, `vols`, `restore`; `?year=` a grant year, or `all` |
| `KKT_Projects_by_Type.html` | The *KKT Projects by Type and Financial Year* chart | none — measure and year are dropdowns |
| `KKT-dashboard.html` | — | none — overview panel for the KKT Overview tab |

Both charts use horizontal bars so the long group names stay readable, and colour by grant year using a fixed, colour-vision-validated palette (a year keeps its colour however many years are shown).

A project carries up to four activities, each in its own field with its own funding figure (`ProjectType` … `ProjectType_4`, `ProjectType_fund` … `ProjectType_fund4`). The export folds them into an `activities` list per project, so the by-type chart sums the team's actual per-activity figures. The parts deliberately need not add up to the grant — the team allocated only money actually spent — and a funding value of 0 means the activity happened with no grant money against it, so it is kept rather than dropped.

### Counting different projects

Each grant row carries a `Project_ID` (`KKT-001` …), assigned once per project and repeated on every grant that project has received, however many years apart. "Different projects funded" on the overview is a distinct count of that field, which a count of project names cannot do reliably — projects get renamed between years.

The codes are assigned by a helper script outside the repo. It is safe to re-run: existing codes are kept and only new grant rows are filled, but check its output each year — a project that gets a new code when it should have matched an existing one silently inflates the count.

### Data entry workbook

`KKT_Workbook_Export.py` and `KKT_Workbook_Load.py` are how the biodiversity team maintains the KKT data. The export builds one Excel workbook holding every grant row and its end-of-year statistics, pre-filled from AGOL; the team edits it; the load diffs it back against the service and reports every changed cell before anything is written.

This replaces exporting a hand-picked subset and re-keying the changes, which is what puts a valid figure against the wrong project.

```
python KKT_Workbook_Export.py                 # build the workbook
python KKT_Workbook_Load.py                   # dry run, reports every change
python KKT_Workbook_Load.py --push            # apply
```

The export only reads AGOL. The load is a dry run by default and writes an outcome CSV either way, naming each changed cell with its before and after value.

#### One row per grant

The projects layer and the statistics table are a 1:1 relationship, so both sit on one flat sheet: key columns, project columns, statistics columns, then a few columns only needed when adding a project. 176 grant rows, 102 of which carry statistics — **the other 74 have blank statistics cells, and filling them in is how a new statistics record gets created.** Nothing has to be matched to anything.

The sheet is protected: the two key columns are locked, columns cannot be inserted or reordered, and sorting is blocked because sorting one column alone would decouple values from their OBJECTID. Filtering stays available. Dropdowns are generated from the layer's own coded-value domains, so a value that AGOL would reject cannot be typed in.

Three statistics fields are deliberately absent from the workbook because the loader derives them: `ProjectID` (the parent GlobalID), `Grant_year` (mirrors `Grant_year` on the same row), and `Applicant` (a free-text copy of the group name that drifts between years — `Group_name` is the canonical one).

`Report_Link` is also absent. `KKT_Report_Link_Build.py` owns it.

#### Two fields retired, September 2026

Building the workbook surfaced two fields on the projects layer that were not worth asking anyone to maintain, and both were removed:

- **`Project`** held `KKT` on all 176 rows. A column where every cell says the same thing tells a reader nothing.
- **`Prev_Act_Grant_1`** ("Previous or Active Grantee") was hand-maintained and could not be kept right. 152 of 176 rows said `Active`, including every 21-22 row — grants that closed four years earlier. It matched neither meaning it could have had: 124 `Active` rows were not in the current grant year, and 56 of 176 disagreed with "this is the project's most recent grant".

  The deeper problem was the grain. The layer holds one row per project **per grant year**, so a 21-22 row is a closed historical record and cannot be active — "active" describes a project, but the field lived on a grant. Nothing displayed it: not the Arcade popup, not any chart, not the Experience Builder app. It was, however, being written into the public `dashboard_data.json`, so a value wrong on about a third of rows was being published.

  Which projects are currently running is still answerable — a project is running if it has a row in the current grant year, from `Project_ID` and `Grant_year`. Nothing stored, nothing to go stale.

The values as they stood are preserved in `Data/KKT data/KKT_workbook_PREPUSH_backup_20260915.csv`, keyed by OBJECTID.

#### Adding a project

Use the blank rows at the bottom and leave the key columns empty. Geometry is resolved in three steps:

| | |
|---|---|
| Latitude and Longitude filled in | the point is built from them, projected from WGS84 to the layer's NZTM2000 |
| `Project_ID` picked from the dropdown | the point is copied from that project's most recent grant year — the common case, a project funded again |
| neither | the row is held and written to `KKT_new_projects_need_geometry.csv` to be placed by hand |

A row that cannot be resolved is skipped whole; nothing is ever half-written. New projects are created before their statistics, because a statistics record needs its parent's GlobalID and that does not exist until the project does.

`Project_ID` is left blank on a genuinely new project — `KKT_Project_ID_Assign.py` fills it afterwards.

#### Guards worth knowing about

- **Loading the same workbook twice** is refused. A new row whose group, grant year and project name already exist on the layer is held, naming the OBJECTID it clashes with. `--allow-duplicates` overrides it for a genuine second grant.
- **Latitude and longitude the wrong way round** is caught — a point outside New Zealand is held rather than created.
- **A cell that is not a plain number** is read where there is one clear number in it (`650 plants` → 650) and the reading is logged. A cell with no single number, or more than one (`40-50`), is reported and left out rather than mangled into 4050.
- **Deleting a row from the workbook does nothing.** Deletions stay a deliberate act in AGOL.
- The script never creates a field. A workbook column with no field on the service stops the run.

After a load that created anything, export a fresh workbook — the old one does not know the OBJECTIDs of the rows it created.

### Logging

Log files are written to `logs/kkt/` (gitignored).

---

## Pressure Management scripts

`Pressure_Management_Data_Join.py` reads a manually exported CSV from the Pressure Management Reporting System SharePoint spreadsheet, joins it to the Priority Habitats spatial layer on AGOL, and outputs:

- Refreshed features pushed straight to the `PH_Pressure_Management` layer and `PH_Pressure_Scores` table on AGOL (delete-all + add, controlled by the `PUSH_TO_AGOL` flag)
- A local file geodatabase with the feature class and related table
- Summary CSVs to the network (consumed by `PM_Dashboard_Export.py`)

### Setup

1. Copy `config.example.py` to `config.py` and fill in your local paths
2. Run from the ArcGIS Pro Python environment (`arcgispro-py3`):

```
pip install arcgis pandas numpy
```

3. Ensure you are signed in to ArcGIS Pro with your portal credentials before running

### Scripts

| Script | Purpose |
|---|---|
| `Pressure_Management_Data_Join.py` | Reads the SharePoint CSV, joins to the spatial layer, updates AGOL, writes the GDB and network CSVs |
| `PM_Dashboard_Export.py` | Reads the network CSVs, builds `dashboard_data.json`, commits and pushes it to GitHub Pages |

Run `Pressure_Management_Data_Join.py` first, then `PM_Dashboard_Export.py`. The HTML chart files are static — they fetch `dashboard_data.json` at runtime from GitHub Pages and require no regeneration.

### Site programme

Each site is classified as **Priority Habitat**, **Icon Site**, or **Regional Park** via the `site_programme` field — the three programmes are funded separately. The six pressure charts carry an *Exclude Icon Sites & Regional Park* filter, so reporting can be viewed for Priority Habitats alone or for all sites together.

### Logging

Each script run writes a timestamped log file to `logs/pressure-management/`. Output is written to both the log file and the console. The `logs/` folder is gitignored.

---

## Targeted Rates scripts

`Targeted_Rates_Data_Export.py` reads the Biodiversity Targeted Rates contractor data
view on AGOL (a filtered view of the main contractor layer, layer 1 waypoints and layer
2 tracks) and rewrites the data block in each project page:

| Page | Project | Data block |
|---|---|---|
| `html/targeted-rates/WBP.html` | Waitārere Beach Community Project | `/* WBP_DATA_START */ … /* WBP_DATA_END */` |
| `html/targeted-rates/REG.html` | Rangitīkei Environment Group | `/* REG_DATA_START */ … /* REG_DATA_END */` |

`Targeted-rate.html` is the static cover page; its reporting period is typed in by hand.

```
python Targeted_Rates_Data_Export.py        # about 20 seconds
```

The script writes the two HTML files only — it does not commit or push.
`Run_All_Updates.py --only targeted-rates` runs it and pushes both pages.

### What each page counts

- **WBP** charts plants (every waypoint is one plant), grouped into Tree Lupin, Sydney
  Golden Wattle and Other. The control outcome is read out of the free-text
  `Control_notes` field, testing for "Partially controlled" and "Not controlled" before
  "Controlled" so the longer phrases are not swallowed by the shorter one. Life stage
  folds the seedling and juvenile classes together.
- **REG** charts recorded points rather than plants, old man's beard only, by reserve and
  age class. `SiteName` spellings that refer to the same reserve are merged through
  `RESERVE_ALIASES`; `SiteID` cannot be used for grouping because one ID covers three
  reserves. Species names are compared with the curly apostrophe straightened, because
  both `'` and `’` appear in the data.

### REG 2024-25 comes from the annual report

REG's 2024-25 spatial data is too patchy to chart (one reserve, no age classes), so that
season is taken from the group's own annual report instead. The figures sit in
`REG_REPORT` in `REG.html`, **outside** the marker block so the script never touches
them, and `REPORT_SEASONS` makes the script drop spatial points for that year. The season
stays its own entry in the dropdown and is never merged with a spatial season; the page
notes that the counts are estimated and not comparable.

### Guards

- A project with no points returned from AGOL is treated as suspect, not as zero: the
  page is left as it was and a warning is logged.
- A `FinYr` value that is not a real financial year (a `24-26` style typo) is skipped
  with a warning.
- A missing marker comment raises rather than warning, so a renamed block cannot
  silently stop updating.
- The data blocks carry no generated timestamp, so a run with unchanged data changes
  nothing and the runner makes no commit.

### Setup

Add to `config.py`:
- `TARGETED_RATES_CONTRACTOR_URL` — FeatureServer URL for the Targeted Rates contractor
  data view (needs a signed-in session; the REST URL returns "Token Required" otherwise)

### Logging

Log files are written to `logs/targeted-rates/` (gitignored).

---

## Tōtara Reserve script

`Totara_Reserve_Data_Export.py` feeds the Tōtara Reserve Regional Park app. Unlike KKT
and Pressure Management, **the data is injected into the HTML rather than fetched at
runtime** — the same pattern as the icon sites — so nothing on a page changes until the
script runs. It does not commit or push; `Run_All_Updates.py` does that.

The script is divided into five sections, each run on its own with `--only`, because they
share nothing and the slow ones should not drag the others along:

| `--only` | Pages written | Sources | Needs ArcGIS |
|---|---|---|---|
| `pco` | `pest-animal-header.html`, `predator-control.html` | Animal Pest Control layer (traps + inspections), possum bait station layer, tracking tunnel spreadsheets, pest animal baiting workbook | Yes |
| `plants` | `pest-plant-header.html`, `pest-plant-control.html`, `pest-plant-legend.html` | Tōtara Reserve contractor data layer and web map | Yes |
| `bio` | `biodiversity.html` | eBird hotspots, iNaturalist, bat detector exports, five-minute bird counts, tracking tunnel spreadsheets | No |
| `camp` | `campground.html` | Newbook booking system REST API | No |
| `river` | `river-management.html` | LAWA swim quality, Horizons Hilltop river level | No |

```
python Totara_Reserve_Data_Export.py --only pco      # about 3 minutes
python Totara_Reserve_Data_Export.py --only bio      # about 2 minutes
```

Set `PYTHONIOENCODING=utf-8` when running by hand: the log lines contain macrons and the
console encoding otherwise chokes.

Run one section at a time. `--only river` writes `river-management.html`, which is not
embedded in the app, so it is left out of the scheduled runs.

### Pages

| Page | What it is |
|---|---|
| `totara-reserve.html` | Cover page, embedded on the app's main page. Cards link to each app page. Scales its own root font size to fit the iframe |
| `pest-animal-header.html` / `predator-control.html` | Two embeds sharing one `PCO_DATA` block: a title strip with five tiles, and the full-width page beneath it (trapping, bait stations, catches by financial year, deer card, rodent tracking index, possum RTCI) |
| `pest-plant-header.html` / `pest-plant-control.html` / `pest-plant-legend.html` | Three embeds sharing one `PEST_PLANT_DATA` block: title strip, charts panel beside the map, and a legend floated over the map |
| `biodiversity.html` | One full-page embed: species recorded in and around the reserve, the five-minute bird count series with per-species trend charts, and bat monitoring |
| `campground.html` | Guest nights, bookings and occupancy by season, month and night |
| `river-management-header.html` | Title strip with a link out to LAWA. LAWA refuses to be iframed, so it is a button rather than an embed |

`fit.js` lays each page out at its Experience Builder design size (`data-base-w` and
`data-base-h` on `<body>`) and scales it as one piece to fit the iframe. The measured
slots are 1111 × 122 for the header strips, 1111 × 564 for the pest animal page and
563 × 564 for the pest plant panel.

### How features are selected

Traps and possum bait stations are chosen by a live spatial intersect against the Tōtara
Reserve polygon (from the HRC Icon Sites layer) every run, not by a name field. The
intersect is sent as a POST: putting a ~1,400-vertex polygon in a GET query string makes
AGOL answer with 502s. Traps use a 200 m buffer because the trap lines run along and just
outside the reserve edge; possum bait stations use none.

### Tracking tunnel index

Two spreadsheets feed the rodent tracking index. The raw per-tunnel records are
authoritative from their first date (May 2022), with the index calculated per survey as
the mean across lines of the share of tunnels tracked. The older summary sheet supplies
everything before that. The chart draws from 2020 on, while the full history stays in the
data.

### Species trend charts

The Species trend tab on the bird card reproduces the Biodiversity team's own charts:
their grouping (a plain mean over the rows present, no per-station summing and no zero
filling) and their `sd / sqrt(26)` standard error, so the figures on the dashboard match
the figures in their reports. Each chart carries the fitted line and its 95% band, the
rat index on a second axis, and named arrows for baiting operations and weather events.
Clicking a species takes the full width of the row. Each chart exports a 1000 × 460 PNG
of the plot alone, for cropping into reports.

Bird count records are read from the master workbook plus any extra workbooks listed in
`TOTARA_5MBC_EXTRA`, skipping a year the master already holds — so once the team merges a
season into the master, the extra workbook stops contributing on its own.

### Hand-maintained content

Figures come from data; sentences and a few things with no feed are entered by hand.

- The deer control card, updated each season from the ICC and contractor reports.
- `TOTARA_EXTRA_EVENTS` in the script, for events the team's baiting workbook does not
  record (it holds operations only). Delete an entry once it appears in their sheet.
- `EVENTS` in `campground.html`, for closures and weather events on the daily chart, and
  `CAMP_CLOSURES` and `CAMP_REMOVED_SITES` in the script — Newbook drops a removed site
  from `sites_list` with no end date, so removals have to be recorded to keep the
  occupancy denominator right.

### Guards

- No boundary returned, or zero features, is a warning and a skipped section, not a zero.
  The page keeps its last good data.
- A bird or pest plant species name the script does not recognise is logged as a warning
  and left out rather than guessed at.
- If the species list or the plant list comes back empty, or any fetch throws, the page is
  left exactly as it was.

### Setup

Add to `config.py` (see `config.example.py` for the full list with comments):
- `TOTARA_TTI_XLSX`, `TOTARA_TTI_RAW_XLSX` — tracking tunnel summary and raw records
- `TOTARA_5MBC_XLSX`, `TOTARA_5MBC_SHEET`, `TOTARA_5MBC_EXTRA` — five-minute bird counts
- `TOTARA_BAT_CSV_GLOB` — bat detector exports, one file per survey
- `TOTARA_PEST_OPS_XLSX`, `TOTARA_PEST_OPS_SHEET` — baiting operations and possum RTCI
- `EBIRD_API_KEY`, `TRAP_SERVICE_URL`, `HRC_ICON_SITES_URL` — shared with the icon sites

Every local path and item ID lives in `config.py`, which is not committed. Newbook
credentials are read from a local file that is not in the repo and are never printed or
logged. Only counts are kept from a booking — dates, site category and age group. Guest
details and money are dropped on arrival and never stored.

### Logging

Log files are written to `logs/totara-reserve/` (gitignored). After a run, check for
`WARNING` and `ERROR` lines before pushing.

