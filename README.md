# biod-hub

Repository for the Biodiversity Programme Hub — HTML dashboard content embedded in AGOL Experience Builder, and the Python automation scripts that process and publish biodiversity programme data.

## Repository structure

```
biod-hub/
├── html/
│   ├── index.html                               # Main hub landing page
│   ├── pressure-management/
│   │   ├── pressure-management-dashboard.html   # Scoring criteria reference
│   │   ├── PH_Pressure_Scores_Overview.html     # Chart — loads dashboard_data.json at runtime
│   │   ├── PH_Pressure_by_Ecosystem.html        # Chart — loads dashboard_data.json at runtime
│   │   ├── PH_Sites_Threshold_Status.html       # Chart — loads dashboard_data.json at runtime
│   │   ├── PH_Pressure_Scores_by_District.html  # Chart — loads dashboard_data.json at runtime
│   │   ├── PH_Sites_YoY_Threshold.html          # Chart — loads dashboard_data.json at runtime
│   │   └── dashboard_data.json                  # Auto-updated by PM_Dashboard_Export.py
│   ├── kkt/
│   │   └── KKT-dashboard.html
│   ├── targeted-rates/
│   │   ├── Targeted-rate.html
│   │   ├── REG.html
│   │   └── WBP.html
│   ├── icon-sites/
│   │   ├── icon-sites.html
│   │   ├── bushy-park.html
│   │   ├── te-apiti.html                        # Auto-updated by Icon_Sites_Data_Export.py
│   │   └── kia-wharite.html                     # Auto-updated by Icon_Sites_Data_Export.py (PCO RTCI from local GDB)
│   └── totara-reserve/                          # Placeholder for future content
├── Pressure_Management_Data_Join.py             # Pressure Management data pipeline
├── PM_Dashboard_Export.py                       # Builds dashboard_data.json and pushes to GitHub
├── Icon_Sites_Data_Export.py                    # Queries AGOL, updates icon site HTML dashboards
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

## Icon Sites script

`Icon_Sites_Data_Export.py` queries two AGOL feature layers — the BioD Contractor Data layer (pest plant control records) and the Animal Pest Control layer (trap network and inspection records) — processes the data per icon site, and injects the results directly into the corresponding HTML dashboard as a `const DATA = {...}` block. The updated HTML is then committed and pushed to GitHub Pages automatically.

### Data sources

| Source | Content | Config key |
|---|---|---|
| BioD Contractor Data feature layer (AGOL) | Waypoints (weed locations) and polylines (track coverage) | `CONTRACTOR_ITEM_ID` |
| Animal Pest Control Layer New (AGOL FeatureServer) | Trap network features and related inspection/catch records | `TRAP_SERVICE_URL` |
| Kia Whārite Project GDB (local network path) | PCO treatment area polygons with Residual Trap Catch Index (RTCI) results | `KIA_WHARITE_GDB` |

### Per-site outputs

| Site | HTML file | Data source | Notes |
|---|---|---|---|
| Te Āpiti – Manawatū Gorge | `html/icon-sites/te-apiti.html` | AGOL (BioD Contractor Data + Animal Pest Control) | Pest plant and trap catch data; all-years summary CSV written to `ICON_SITES_OUTPUT_DIR` |
| Kia Whārite | `html/icon-sites/kia-wharite.html` | Local GDB (`KIA_WHARITE_GDB`) | PCO RTCI values from `PCO_Treatment_Area_ExportFeatures`; trap catch and weed data are static (from annual reports) |

### Setup

1. Copy `config.example.py` to `config.py` and fill in the required keys:
   - `CONTRACTOR_ITEM_ID` — AGOL item ID for the BioD Contractor Data feature layer
   - `TRAP_SERVICE_URL` — FeatureServer URL for the Animal Pest Control layer
   - `KIA_WHARITE_GDB` — network path to the Kia Whārite Project File Geodatabase
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

## Pressure Management scripts

`Pressure_Management_Data_Join.py` reads a manually exported CSV from the Pressure Management Reporting System SharePoint spreadsheet, joins it to the Priority Habitats spatial layer on AGOL, and outputs:

- Updated features to the `PH_Pressure_Management` AGOL feature service
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

### Logging

Each script run writes a timestamped log file to `logs/pressure-management/`. Output is written to both the log file and the console. The `logs/` folder is gitignored.

