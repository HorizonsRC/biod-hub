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
│   │   ├── PH_Site_Pressure_Summary.html        # Per-site pressure card (?site= URL param)
│   │   ├── PH_Pressure_Icons.html               # Static pressure-type image panel
│   │   └── dashboard_data.json                  # Auto-updated by PM_Dashboard_Export.py
│   ├── kkt/
│   │   ├── KKT-dashboard.html                   # Overview panel — loads dashboard_data.json
│   │   ├── KKT_Project_Stats.html               # Chart — loads dashboard_data.json (?metric=, ?year=)
│   │   ├── KKT_Projects_by_Type.html            # Chart — loads dashboard_data.json
│   │   └── dashboard_data.json                  # Auto-updated by KKT_Dashboard_Export.py
│   ├── targeted-rates/
│   │   ├── Targeted-rate.html
│   │   ├── REG.html
│   │   └── WBP.html
│   ├── icon-sites/
│   │   ├── icon-sites.html
│   │   ├── bushy-park.html
│   │   ├── te-apiti.html                        # Auto-updated by Icon_Sites_Data_Export.py
│   │   ├── kia-wharite.html                     # Auto-updated by Icon_Sites_Data_Export.py (PCO RTCI from local GDB)
│   │   ├── manawatu-estuary.html                # Auto-updated by Icon_Sites_Data_Export.py
│   │   ├── pukaha.html                          # Auto-updated by Icon_Sites_Data_Export.py
│   │   └── ruahine-kiwi.html                   # Auto-updated by Icon_Sites_Data_Export.py (Trap.NZ + local GDB)
│   └── totara-reserve/                          # Placeholder for future content
├── KKT_Stats_Update.py                          # Loads the KKT stats spreadsheet into AGOL
├── KKT_Dashboard_Export.py                      # Builds html/kkt/dashboard_data.json
├── Pressure_Management_Data_Join.py             # Pressure Management data pipeline
├── PM_Dashboard_Export.py                       # Builds dashboard_data.json and pushes to GitHub
├── Icon_Sites_Data_Export.py                    # Queries AGOL, updates icon site HTML dashboards
├── Hub_Stats_Export.py                          # Builds html/hub_stats.json (landing page figures)
├── config.py                                    # Local paths — gitignored, not committed
├── config.example.py                            # Template for config.py
└── requirements.txt
```

HTML files are served via GitHub Pages at `https://HorizonsRC.github.io/biod-hub/`.

## Programmes

| Programme | Dashboard files |
|---|---|
| Priority Habitats Pressure Management | `html/pressure-management/` |
| KKT Fund | `html/kkt/` |
| Targeted Rates | `html/targeted-rates/` |
| Icon Sites | `html/icon-sites/` |
| Tōtara Reserve | `html/totara-reserve/` *(coming soon)* |

## Hub landing page stats

`Hub_Stats_Export.py` builds `html/hub_stats.json`, which `index.html` fetches at runtime to fill the "Programme at a Glance" strip and the header's reporting-period badge. Same arrangement as the other dashboards — the HTML is static and never regenerated.

```bash
python Hub_Stats_Export.py            # write the JSON
python Hub_Stats_Export.py --push     # write, commit and push to GitHub Pages
```

| Figure | Source |
|---|---|
| Priority Habitat sites | `Priority_Habitats_Pressure_Management` layer 0, distinct `site_id` where `site_programme = 'Priority Habitat'` — so the Icon Sites and Tōtara Reserve carried in the same layer are excluded |
| KKT grants | `Biodiversity_KKT_Projects` layer 4 row count — grants awarded across all years, not distinct projects |
| Reporting period | The financial year just finished, derived from today's date (NZ FYs run 1 July – 30 June) |

The figures currently in the HTML are fallbacks: if the JSON fails to load, the page shows them rather than rendering blank. They go stale, so run this script whenever the underlying data changes — after `Pressure_Management_Data_Join.py` or `KKT_Stats_Update.py`, and at the start of each financial year.

`PH_PRESSURE_ITEM_ID` in `config.py` is the AGOL item ID of the published pressure management service.

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
| `NAME_ALIASES` | Spreadsheet group name → `Group_name_1`, for groups the fuzzy match cannot reach. |
| `ROW_OVERRIDES` | Spreadsheet row number → exact `ProjectNam_1`, for groups with several projects that the sheet gives no project name for. Keyed by grant year. |

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

`KKT_Related_Table_Statistics` has a `Grant_year` field (text, 5 — e.g. `25_26`) mirroring `Grant_year_1` on the projects layer. The table has no other year marker, so without it nothing can split the stats by financial year. `KKT_Stats_Update.py` stamps it on every row it writes; to fix rows loaded before the field existed:

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

The export joins each stats row to its project and adds `group_name`, `project_name` and `district`. Charts group by `group_name` (the canonical `Group_name_1`) rather than the stats table's `Applicant`, which is free text copied from the spreadsheet and spells the same group differently year to year — grouping by `Applicant` splits one group into several bars. It also parses `Current_YearFund` (`'$3,763.00 '`) into a numeric `funding` field.

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

