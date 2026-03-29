# biod-hub

Repository for the Biodiversity Programme Hub — HTML dashboard content embedded in AGOL Experience Builder, and the Python automation script that processes and publishes Priority Habitats Pressure Management data.

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
│   │   └── dashboard_data.json                  # Auto-updated by PM_Dashboard_Export.py
│   ├── kkt/
│   │   └── KKT-dashboard.html
│   ├── targeted-rates/
│   │   ├── Targeted-rate.html
│   │   ├── REG.html
│   │   └── WBP.html
│   ├── icon-sites/
│   │   ├── icon-sites.html
│   │   └── bushy-park.html
│   └── totara-reserve/                          # Placeholder for future content
├── Pressure_Management_Data_Join.py             # Main data pipeline
├── PM_Dashboard_Export.py                       # Builds dashboard_data.json and pushes to GitHub
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

## Pressure Management script

`Pressure_Management_Data_Join.py` reads a manually exported CSV from the Pressure Management Reporting System SharePoint spreadsheet, joins it to the Priority Habitats spatial layer on AGOL, and outputs:

- Updated features to the `PH_Pressure_Management` AGOL feature service
- A local file geodatabase with the feature class and related table
- Three HTML chart files written to `html/pressure-management/`
- Summary CSVs to the network

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

Future scripts will write to their own subfolder under `logs/`.

