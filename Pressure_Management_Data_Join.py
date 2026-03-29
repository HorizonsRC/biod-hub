import os
import logging
from datetime import datetime as dt

# Set up logging — must come before other imports so any import errors are captured
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "pressure-management")
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, dt.now().strftime('%Y-%m-%d_%H-%M-%S') + '.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

log.info("Starting Pressure Management Data Join")
log.info(f"Log file: {log_file}")

import pandas as pd
import numpy as np
from arcgis.features import FeatureLayer
import arcpy
from config import CSV_PATH, OUTPUT_GDB, NETWORK_DIR, OUTPUT_SUMMARY_CSV

log.info("Libraries imported successfully")

# ArcGIS Server REST Endpoint
FEATURE_SERVICE_URL = "https://services1.arcgis.com/VuN78wcRdq1Oj69W/arcgis/rest/services/Priority_habitats_with_PCO/FeatureServer/0"

OUTPUT_MAIN_FC = "PH_Pressure_Management"
OUTPUT_RELATED_TABLE = "PH_Pressure_Scores_TimeSeries"

log.info(f"Feature Service URL: {FEATURE_SERVICE_URL}")
log.info(f"CSV Path: {CSV_PATH}")
log.info(f"Output GDB: {OUTPUT_GDB}")
log.info(f"Output Main FC: {OUTPUT_MAIN_FC}")
log.info(f"Output Related Table: {OUTPUT_RELATED_TABLE}")
log.info(f"Output Summary CSV: {OUTPUT_SUMMARY_CSV}")

from arcgis.gis import GIS

log.info("Connecting to feature service using ArcGIS Pro sign-in...")
gis = GIS("home")
feature_layer = FeatureLayer(FEATURE_SERVICE_URL, gis=gis)
log.info("Connected to feature service")

log.info("Querying all features...")
features = feature_layer.query(
    where="1=1",
    out_fields="*",
    return_geometry=True
)

spatial_data = features.sdf

log.info(f"Retrieved {len(spatial_data)} features")
log.info(f"Spatial data shape: {spatial_data.shape}")
log.info(f"First few rows:\n{spatial_data[['SiteID', 'SiteName', 'Actual_EcoSystem_s_', 'AreaHa']].head().to_string()}")

# Load local CSV (manual export from SharePoint - Raw site data tab)
log.info("Loading pressure data CSV...")
raw_csv = pd.read_csv(CSV_PATH, header=3)

# Extract left side (columns A-M)
left_side = raw_csv.iloc[:, 0:13].copy()
left_side.columns = ['Region', 'FY', 'SiteID', 'SiteName', 'Ecosystem', 'Weighted_Y_N', 'Lead',
                     'Raw_Ungulates', 'Raw_PestPlants', 'Raw_PossumBrowse', 'Raw_Predation',
                     'Raw_Environmental', 'Raw_Rabbits']

# Extract right side (columns P-Y)
right_side = raw_csv.iloc[:, 15:25].copy()
right_side.columns = ['SiteID_check', 'Weighted_Ungulates', 'Weighted_PestPlants', 'Weighted_PossumBrowse',
                      'Weighted_Predation', 'Weighted_Environmental', 'Weighted_Rabbits',
                      'Total_Score', 'Average_Score', 'Pct_Above_Threshold']

# Remove empty rows
left_clean = left_side[left_side['SiteID'].notna()].copy()
right_clean = right_side[right_side['SiteID_check'].notna()].copy()

# Join left and right sides
pressure_data = left_clean.reset_index(drop=True).join(right_clean.reset_index(drop=True))
pressure_data = pressure_data.drop('SiteID_check', axis=1)

# Convert numeric columns
numeric_cols = ['Raw_Ungulates', 'Raw_PestPlants', 'Raw_PossumBrowse', 'Raw_Predation',
                'Raw_Environmental', 'Raw_Rabbits', 'Weighted_Ungulates', 'Weighted_PestPlants',
                'Weighted_PossumBrowse', 'Weighted_Predation', 'Weighted_Environmental',
                'Weighted_Rabbits', 'Total_Score']

for col in numeric_cols:
    pressure_data[col] = pd.to_numeric(pressure_data[col], errors='coerce')

# Fix case sensitivity issues
pressure_data['SiteID'] = pressure_data['SiteID'].replace({
    'Rang187A': 'Rang187a',
    'Rang72a': 'Rang72A'
})

# Calculate threshold flag
pressure_data['Above_55_Threshold'] = (pressure_data['Total_Score'] >= 55).astype(int)

log.info(f"Pressure data shape: {pressure_data.shape}")
log.info(f"Financial Years in data: {sorted(pressure_data['FY'].unique())}")
log.info(f"First few rows:\n{pressure_data[['SiteID', 'SiteName', 'FY', 'Total_Score', 'Above_55_Threshold']].head(10).to_string()}")

log.info("=" * 70)
log.info("JOIN VALIDATION")
log.info("=" * 70)

spatial_ids  = set(spatial_data['SiteID'].dropna())
pressure_ids = set(pressure_data['SiteID'].dropna())

log.info(f"Spatial sites:  {len(spatial_ids)}")
log.info(f"Pressure sites: {len(pressure_ids)}")

matched           = spatial_ids.intersection(pressure_ids)
unmatched_spatial = spatial_ids - pressure_ids
unmatched_pressure = pressure_ids - spatial_ids

log.info(f"Matched: {len(matched)}")

if unmatched_spatial:
    log.warning(f"Spatial sites NOT in pressure data ({len(unmatched_spatial)}): {sorted(unmatched_spatial)}")

if unmatched_pressure:
    log.warning(f"Pressure sites NOT in spatial data ({len(unmatched_pressure)}): {sorted(unmatched_pressure)}")

if not unmatched_spatial and not unmatched_pressure:
    log.info("Perfect match — all sites joined successfully")

log.info(f"Match rate: {len(matched)} / {len(spatial_ids)} = {100*len(matched)/len(spatial_ids):.1f}%")

# Main feature class - spatial data only
main_fc_data = spatial_data[[
    'SiteID', 'SiteName', 'Actual_EcoSystem_s_', 'AreaHa',
    'HRCLevel', 'HRCStaff', 'Management', 'Protection',
    'SHAPE'
]].copy()

main_fc_data = main_fc_data.rename(columns={'Actual_EcoSystem_s_': 'Ecosystem'})

log.info("Adding latest pressure scores to main feature class...")

latest_scores = pressure_data.sort_values('FY').drop_duplicates(subset=['SiteID'], keep='last')[
    ['SiteID', 'Total_Score', 'Above_55_Threshold', 'FY']
]

main_fc_data = main_fc_data.merge(latest_scores, on='SiteID', how='left')

log.info(f"Main feature class — Shape: {main_fc_data.shape}, Unique sites: {main_fc_data['SiteID'].nunique()}")
log.info(f"First few rows:\n{main_fc_data[['SiteID', 'SiteName', 'Ecosystem', 'AreaHa', 'Total_Score', 'Above_55_Threshold', 'FY']].head().to_string()}")

# Related table - one row per site per financial year
related_table_data = pressure_data[[
    'SiteID', 'FY', 'Region', 'Ecosystem',
    'Raw_Ungulates', 'Raw_PestPlants', 'Raw_PossumBrowse', 'Raw_Predation',
    'Raw_Environmental', 'Raw_Rabbits',
    'Weighted_Ungulates', 'Weighted_PestPlants', 'Weighted_PossumBrowse',
    'Weighted_Predation', 'Weighted_Environmental', 'Weighted_Rabbits',
    'Total_Score', 'Above_55_Threshold', 'Lead'
]].copy()

log.info(f"Related table — Shape: {related_table_data.shape}, Unique sites: {related_table_data['SiteID'].nunique()}, Financial years: {sorted(related_table_data['FY'].unique())}")

# Summary table - breakdown by District AND Financial Year
log.info("Creating summary by District and Financial Year...")

summary = pressure_data.groupby(['Region', 'FY']).agg({
    'SiteID': 'count',
    'Total_Score': 'mean',
    'Above_55_Threshold': 'sum'
}).reset_index()

summary.columns = ['District', 'Financial_Year', 'No_of_Sites', 'Average_Score', 'Sites_Above_Threshold']
summary['Sites_Below_Threshold'] = summary['No_of_Sites'] - summary['Sites_Above_Threshold']
summary['Pct_Above_Threshold']   = (summary['Sites_Above_Threshold'] / summary['No_of_Sites'] * 100).round(1)
summary['Pct_Below_Threshold']   = (summary['Sites_Below_Threshold'] / summary['No_of_Sites'] * 100).round(1)
summary['Average_Score']         = summary['Average_Score'].round(2)
summary = summary[[
    'District', 'Financial_Year', 'No_of_Sites', 'Average_Score',
    'Sites_Above_Threshold', 'Sites_Below_Threshold', 'Pct_Above_Threshold', 'Pct_Below_Threshold'
]].copy()
summary = summary.sort_values(['District', 'Financial_Year']).reset_index(drop=True)

log.info(f"Summary by District and Financial Year:\n{summary.to_string(index=False)}")

# Pressure by Ecosystem Summary
log.info("Creating pressure by ecosystem and financial year summary...")

pressure_mapping = {
    'Raw_Ungulates':     'Ungulates',
    'Raw_PestPlants':    'Pest plants',
    'Raw_PossumBrowse':  'Possum browse',
    'Raw_Predation':     'Predation',
    'Raw_Environmental': 'Fragmentation',
    'Raw_Rabbits':       'Rabbits/hares'
}

pressure_ecosystem_data = []

for ecosystem in sorted(pressure_data['Ecosystem'].unique()):
    for fy in sorted(pressure_data['FY'].unique()):
        eco_fy_data = pressure_data[(pressure_data['Ecosystem'] == ecosystem) & (pressure_data['FY'] == fy)]
        if len(eco_fy_data) > 0:
            for raw_col, pressure_name in pressure_mapping.items():
                pressure_ecosystem_data.append({
                    'Region': 'All Regions',
                    'Ecosystem': ecosystem,
                    'Financial_Year': fy,
                    'Pressure_Type': pressure_name,
                    'Average_Score': round(eco_fy_data[raw_col].mean(), 2)
                })

for region in sorted(pressure_data['Region'].dropna().unique()):
    for ecosystem in sorted(pressure_data['Ecosystem'].unique()):
        for fy in sorted(pressure_data['FY'].unique()):
            region_eco_fy_data = pressure_data[
                (pressure_data['Region'] == region) &
                (pressure_data['Ecosystem'] == ecosystem) &
                (pressure_data['FY'] == fy)
            ]
            if len(region_eco_fy_data) > 0:
                for raw_col, pressure_name in pressure_mapping.items():
                    pressure_ecosystem_data.append({
                        'Region': region,
                        'Ecosystem': ecosystem,
                        'Financial_Year': fy,
                        'Pressure_Type': pressure_name,
                        'Average_Score': round(region_eco_fy_data[raw_col].mean(), 2)
                    })

pressure_by_ecosystem = pd.DataFrame(pressure_ecosystem_data)
pressure_by_ecosystem['Region_Ecosystem'] = pressure_by_ecosystem['Region'] + ' ' + pressure_by_ecosystem['Ecosystem']
pressure_by_ecosystem = pressure_by_ecosystem[[
    'Region_Ecosystem', 'Region', 'Ecosystem', 'Financial_Year', 'Pressure_Type', 'Average_Score'
]].copy()

log.info(f"Pressure by Ecosystem:\n{pressure_by_ecosystem.to_string(index=False)}")

pressure_ecosystem_csv = os.path.join(NETWORK_DIR, "PH_Pressure_by_Ecosystem.csv")
pressure_by_ecosystem.to_csv(pressure_ecosystem_csv, index=False)
log.info(f"Saved: {pressure_ecosystem_csv}")

# Sites Above/Below Threshold by Region, Ecosystem and Financial Year
log.info("Creating sites above/below threshold by region, ecosystem and FY...")

threshold_data = []

for region in sorted(pressure_data['Region'].dropna().unique()):
    for ecosystem in sorted(pressure_data['Ecosystem'].unique()):
        for fy in sorted(pressure_data['FY'].unique()):
            region_eco_fy_data = pressure_data[
                (pressure_data['Region'] == region) &
                (pressure_data['Ecosystem'] == ecosystem) &
                (pressure_data['FY'] == fy)
            ]
            if len(region_eco_fy_data) > 0:
                sites_above    = int(region_eco_fy_data['Above_55_Threshold'].sum())
                sites_below    = len(region_eco_fy_data) - sites_above
                region_ecosystem = f"{ecosystem} {region}"

                threshold_data.append({
                    'Region_Ecosystem': region_ecosystem, 'Region': region,
                    'Ecosystem': ecosystem, 'Financial_Year': fy,
                    'Threshold_Status': 'Above', 'Count': sites_above
                })
                threshold_data.append({
                    'Region_Ecosystem': region_ecosystem, 'Region': region,
                    'Ecosystem': ecosystem, 'Financial_Year': fy,
                    'Threshold_Status': 'Below', 'Count': sites_below
                })

threshold_by_region = pd.DataFrame(threshold_data)

log.info(f"Sites Above/Below Threshold:\n{threshold_by_region.to_string(index=False)}")

threshold_csv = os.path.join(NETWORK_DIR, "PH_Sites_Threshold_Status.csv")
threshold_by_region.to_csv(threshold_csv, index=False)
log.info(f"Saved: {threshold_csv}")

# ============================================================
# ARCPY EXPORTS
# ============================================================

arcpy.env.workspace      = OUTPUT_GDB
arcpy.env.overwriteOutput = True

log.info(f"ArcPy workspace: {OUTPUT_GDB}")

if not os.path.exists(OUTPUT_GDB):
    log.info("Creating geodatabase...")
    arcpy.CreateFileGDB_management(os.path.dirname(OUTPUT_GDB), os.path.basename(OUTPUT_GDB))
    log.info("Geodatabase created")
else:
    log.info("Geodatabase exists")

log.info(f"Exporting main feature class: {OUTPUT_MAIN_FC}")
main_fc_path = os.path.join(OUTPUT_GDB, OUTPUT_MAIN_FC)
main_fc_data.spatial.to_featureclass(main_fc_path, overwrite=True)
log.info("Main feature class exported")

log.info(f"Exporting related table: {OUTPUT_RELATED_TABLE}")
related_table_path = os.path.join(OUTPUT_GDB, OUTPUT_RELATED_TABLE)
temp_csv = os.path.join(OUTPUT_GDB, OUTPUT_RELATED_TABLE + '.csv')
related_table_data.to_csv(temp_csv, index=False)
arcpy.conversion.TableToTable(temp_csv, OUTPUT_GDB, OUTPUT_RELATED_TABLE)
if os.path.exists(temp_csv):
    os.remove(temp_csv)
log.info("Related table exported")

log.info("Exporting summary CSV...")
summary.to_csv(OUTPUT_SUMMARY_CSV, index=False)
log.info("Summary CSV exported")

# ============================================================
# VALIDATION
# ============================================================

log.info("=" * 70)
log.info("VALIDATION")
log.info("=" * 70)

main_fc_count = int(arcpy.management.GetCount(main_fc_path)[0])
log.info(f"Main Feature Class '{OUTPUT_MAIN_FC}': {main_fc_count} features")

related_table_count = int(arcpy.management.GetCount(related_table_path)[0])
log.info(f"Related Table '{OUTPUT_RELATED_TABLE}': {related_table_count} rows")
log.info(f"Fields: {[f.name for f in arcpy.ListFields(related_table_path) if not f.name.startswith('OID')]}")

log.info(f"Summary Table: {len(summary)} rows\n{summary.to_string(index=False)}")

# Create relationship class
log.info("Creating relationship between feature class and related table...")

arcpy.management.CreateRelationshipClass(
    origin_table=main_fc_path,
    destination_table=related_table_path,
    origin_primary_key="site_id",
    origin_foreign_key="SiteID",
    destination_primary_key="SiteID",
    destination_foreign_key="SiteID",
    relationship_type="SIMPLE",
    forward_label="Pressure Scores",
    backward_label="Site Details",
    message_direction="FORWARD"
)

log.info(f"Relationship created: {OUTPUT_MAIN_FC} → {OUTPUT_RELATED_TABLE} (SiteID, 1-to-Many)")

# ============================================================
# GENERATE HTML CHART FILES
# ============================================================

log.info("=" * 70)
log.info("GENERATING HTML CHARTS")
log.info("=" * 70)

CHARTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "html", "pressure-management")
os.makedirs(CHARTS_DIR, exist_ok=True)

generated_at   = dt.now().strftime('%Y-%m-%d %H:%M')
CHARTJS_CDN    = "https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"
ANNOTATION_CDN = "https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.0.1/dist/chartjs-plugin-annotation.min.js"


def write_chart_html(filepath, title, config_json):
    script = f"new Chart(document.getElementById('chart'), {config_json});"
    html = (
        "<!DOCTYPE html>\n<html lang='en'>\n<head>\n"
        f"  <meta charset='UTF-8'><title>{title}</title>\n"
        f"  <script src='{CHARTJS_CDN}'></script>\n"
        f"  <script src='{ANNOTATION_CDN}'></script>\n"
        "  <style>\n"
        "    *{box-sizing:border-box;margin:0;padding:0}\n"
        "    body{font-family:Arial,sans-serif;background:#f0f2f5;padding:24px}\n"
        "    .card{background:#fff;border-radius:8px;padding:24px 24px 12px;box-shadow:0 1px 4px rgba(0,0,0,.15)}\n"
        "    .updated{color:#aaa;font-size:.72em;margin-top:10px;text-align:right}\n"
        "  </style>\n"
        "</head>\n<body>\n"
        "  <div class='card'>\n"
        "    <canvas id='chart'></canvas>\n"
        f"    <p class='updated'>Generated: {generated_at}</p>\n"
        "  </div>\n"
        f"  <script>{script}</script>\n"
        "</body>\n</html>"
    )
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)
    log.info(f"Chart written: {os.path.basename(filepath)}")


def write_ecosystem_chart_html(filepath, df, generated_at):
    """Filterable ecosystem pressure chart with FY and District selectors."""
    data_json = df.to_json(orient='records')

    template = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Average Pressure by Ecosystem Type</title>
  <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
  <script src="%%CHARTJS%%"></script>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:'DM Sans',sans-serif;background:#f0f2f5;padding:24px}
    .wrap{background:#fff;border-radius:10px;padding:24px;box-shadow:0 1px 6px rgba(0,0,0,.12);position:relative}
    .hd{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:20px;gap:16px}
    .title{font-size:1rem;font-weight:600;color:#1a1a2e;line-height:1.4}
    .filter-btn{background:#273747;color:#fff;border:none;border-radius:6px;padding:7px 12px;cursor:pointer;display:flex;align-items:center;gap:6px;font-size:0.78rem;font-family:inherit;white-space:nowrap;flex-shrink:0}
    .filter-btn:hover{background:#3a4f63}
    .panel{display:none;position:absolute;right:24px;top:64px;background:#fff;border-radius:8px;box-shadow:0 4px 24px rgba(0,0,0,.18);z-index:100;min-width:280px;overflow:hidden}
    .panel.open{display:block}
    .panel-hd{background:#273747;color:#fff;padding:12px 16px;display:flex;align-items:center;justify-content:space-between}
    .panel-hd span{font-weight:600;font-size:0.9rem}
    .panel-close{background:none;border:none;color:#fff;cursor:pointer;font-size:1.1rem;line-height:1;opacity:.8}
    .panel-close:hover{opacity:1}
    .panel-body{padding:16px}
    .fg{margin-bottom:14px}
    .fg:last-child{margin-bottom:0}
    .fl{font-size:0.78rem;color:#555;font-weight:500;margin-bottom:6px}
    select{width:100%;border:1px solid #ddd;border-radius:6px;padding:8px 28px 8px 10px;font-size:0.85rem;color:#1a1a2e;font-family:inherit;appearance:none;background:#fff url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%23666' stroke-width='2.5'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E") no-repeat right 10px center;cursor:pointer}
    select:focus{outline:none;border-color:#273747}
    .updated{color:#aaa;font-size:.72em;margin-top:14px;text-align:right}
  </style>
</head>
<body>
<div class="wrap">
  <div class="hd">
    <div class="title">Average Pressure Management Score by Ecosystem Type</div>
    <button class="filter-btn" onclick="togglePanel()">
      <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3"/></svg>
      Filter
    </button>
  </div>

  <div class="panel" id="panel">
    <div class="panel-hd">
      <span>Filter</span>
      <button class="panel-close" onclick="togglePanel()">&#x2715;</button>
    </div>
    <div class="panel-body">
      <div class="fg">
        <div class="fl">Financial Year is</div>
        <select id="fySelect" onchange="updateChart()"></select>
      </div>
      <div class="fg">
        <div class="fl">District is</div>
        <select id="regionSelect" onchange="updateChart()"></select>
      </div>
    </div>
  </div>

  <canvas id="chart"></canvas>
  <p class="updated">Generated: %%GENERATED_AT%%</p>
</div>
<script>
  const rawData      = %%DATA%%;
  const fys          = [...new Set(rawData.map(d => d.Financial_Year))].sort();
  const regions      = [...new Set(rawData.filter(d => d.Region !== 'All Regions').map(d => d.Region))].sort();
  const pressureTypes = [...new Set(rawData.map(d => d.Pressure_Type))].sort();
  const ecosystems   = [...new Set(rawData.map(d => d.Ecosystem))].sort();
  const ecoColors    = {
    'Coastal': '#F9A825', 'C': '#F9A825',
    'Forest':  '#2E7D32', 'F': '#2E7D32',
    'Wetland': '#1565C0', 'W': '#1565C0'
  };

  const fySelect     = document.getElementById('fySelect');
  const regionSelect = document.getElementById('regionSelect');

  // Populate FY dropdown — default to latest FY
  fys.forEach(fy => {
    const o = document.createElement('option');
    o.value = fy; o.textContent = fy;
    fySelect.appendChild(o);
  });
  fySelect.value = fys[fys.length - 1];

  // Populate District dropdown
  [{ v: 'All Regions', t: 'All Regions' }, ...regions.map(r => ({ v: r, t: r }))].forEach(({ v, t }) => {
    const o = document.createElement('option');
    o.value = v; o.textContent = t;
    regionSelect.appendChild(o);
  });

  function median(vals) {
    if (!vals.length) return null;
    const s = [...vals].sort((a, b) => a - b);
    const m = Math.floor(s.length / 2);
    return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
  }

  function getDatasets() {
    const fy     = fySelect.value;
    const region = regionSelect.value;
    let rows = rawData.filter(d => d.Region === region && String(d.Financial_Year) === String(fy));
    return ecosystems.map(eco => ({
      label: eco,
      data: pressureTypes.map(pt => {
        const vals = rows.filter(d => d.Ecosystem === eco && d.Pressure_Type === pt).map(d => d.Average_Score);
        const v = median(vals);
        return v !== null ? Math.round(v * 100) / 100 : null;
      }),
      backgroundColor: ecoColors[eco] || '#888',
      borderRadius: 3
    }));
  }

  const chart = new Chart(document.getElementById('chart'), {
    type: 'bar',
    data: { labels: pressureTypes, datasets: getDatasets() },
    options: {
      responsive: true,
      plugins: {
        legend: { position: 'bottom', labels: { font: { family: "'DM Sans', sans-serif" }, padding: 16 } },
        title:  { display: false }
      },
      scales: {
        x: { grid: { display: false }, ticks: { font: { family: "'DM Sans', sans-serif" } } },
        y: { min: 0, max: 100,
             grid: { color: '#e8e8e8', borderDash: [4, 4] },
             ticks: { font: { family: "'DM Sans', sans-serif" } } }
      }
    }
  });

  function updateChart() { chart.data.datasets = getDatasets(); chart.update(); }
  function togglePanel()  { document.getElementById('panel').classList.toggle('open'); }
</script>
</body>
</html>"""

    html = (template
            .replace('%%CHARTJS%%', CHARTJS_CDN)
            .replace('%%DATA%%', data_json)
            .replace('%%GENERATED_AT%%', generated_at))

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)
    log.info(f"Chart written: {os.path.basename(filepath)}")


def write_threshold_chart_html(filepath, df, generated_at):
    """Filterable stacked bar chart of sites above/below threshold by Region_Ecosystem."""
    data_json = df.to_json(orient='records')

    template = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Sites Above/Below Threshold by District &amp; Ecosystem</title>
  <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
  <script src="%%CHARTJS%%"></script>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:'DM Sans',sans-serif;background:#f0f2f5;padding:24px}
    .wrap{background:#fff;border-radius:10px;padding:24px;box-shadow:0 1px 6px rgba(0,0,0,.12);position:relative}
    .hd{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:20px;gap:16px}
    .title{font-size:1rem;font-weight:600;color:#1a1a2e;line-height:1.4}
    .filter-btn{background:#273747;color:#fff;border:none;border-radius:6px;padding:7px 12px;cursor:pointer;display:flex;align-items:center;gap:6px;font-size:0.78rem;font-family:inherit;white-space:nowrap;flex-shrink:0}
    .filter-btn:hover{background:#3a4f63}
    .panel{display:none;position:absolute;right:24px;top:64px;background:#fff;border-radius:8px;box-shadow:0 4px 24px rgba(0,0,0,.18);z-index:100;min-width:280px;overflow:hidden}
    .panel.open{display:block}
    .panel-hd{background:#273747;color:#fff;padding:12px 16px;display:flex;align-items:center;justify-content:space-between}
    .panel-hd span{font-weight:600;font-size:0.9rem}
    .panel-close{background:none;border:none;color:#fff;cursor:pointer;font-size:1.1rem;line-height:1;opacity:.8}
    .panel-close:hover{opacity:1}
    .panel-body{padding:16px}
    .fg{margin-bottom:14px}
    .fg:last-child{margin-bottom:0}
    .fl{font-size:0.78rem;color:#555;font-weight:500;margin-bottom:6px}
    select{width:100%;border:1px solid #ddd;border-radius:6px;padding:8px 28px 8px 10px;font-size:0.85rem;color:#1a1a2e;font-family:inherit;appearance:none;background:#fff url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%23666' stroke-width='2.5'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E") no-repeat right 10px center;cursor:pointer}
    select:focus{outline:none;border-color:#273747}
    .updated{color:#aaa;font-size:.72em;margin-top:14px;text-align:right}
  </style>
</head>
<body>
<div class="wrap">
  <div class="hd">
    <div class="title">No. of Sites Above/Below Threshold by District &amp; Ecosystem Type</div>
    <button class="filter-btn" onclick="togglePanel()">
      <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3"/></svg>
      Filter
    </button>
  </div>

  <div class="panel" id="panel">
    <div class="panel-hd">
      <span>Filter</span>
      <button class="panel-close" onclick="togglePanel()">&#x2715;</button>
    </div>
    <div class="panel-body">
      <div class="fg">
        <div class="fl">Financial Year is</div>
        <select id="fySelect" onchange="updateChart()"></select>
      </div>
      <div class="fg">
        <div class="fl">District is</div>
        <select id="regionSelect" onchange="updateChart()"></select>
      </div>
    </div>
  </div>

  <canvas id="chart"></canvas>
  <p class="updated">Generated: %%GENERATED_AT%%</p>
</div>
<script>
  const rawData = %%DATA%%;
  const fys     = [...new Set(rawData.map(d => d.Financial_Year))].sort();
  const regions = [...new Set(rawData.map(d => d.Region))].sort();

  const fySelect     = document.getElementById('fySelect');
  const regionSelect = document.getElementById('regionSelect');

  // Populate FY dropdown — default to latest FY
  fys.forEach(fy => {
    const o = document.createElement('option');
    o.value = fy; o.textContent = fy;
    fySelect.appendChild(o);
  });
  fySelect.value = fys[fys.length - 1];

  // Populate Region dropdown
  [{ v: 'All Regions', t: 'All Regions' }, ...regions.map(r => ({ v: r, t: r }))].forEach(({ v, t }) => {
    const o = document.createElement('option');
    o.value = v; o.textContent = t;
    regionSelect.appendChild(o);
  });

  function getChartData() {
    const fy     = fySelect.value;
    const region = regionSelect.value;

    let rows = rawData.filter(d => String(d.Financial_Year) === String(fy));
    if (region !== 'All Regions') rows = rows.filter(d => d.Region === region);

    const labels = [...new Set(rows.map(d => d.Region_Ecosystem))].sort();

    const datasets = ['Below', 'Above'].map(status => ({
      label: status + ' Threshold',
      data: labels.map(re => {
        const match = rows.find(d => d.Region_Ecosystem === re && d.Threshold_Status === status);
        return match ? match.Count : 0;
      }),
      backgroundColor: status === 'Above' ? '#43A047' : '#1565C0',
      stack: 'threshold',
      borderRadius: 3
    }));

    return { labels, datasets };
  }

  const { labels: initLabels, datasets: initDatasets } = getChartData();

  const chart = new Chart(document.getElementById('chart'), {
    type: 'bar',
    data: { labels: initLabels, datasets: initDatasets },
    options: {
      responsive: true,
      plugins: {
        legend: { position: 'bottom', labels: { font: { family: "'DM Sans', sans-serif" }, padding: 16 } },
        title:  { display: false }
      },
      scales: {
        x: { stacked: true, grid: { display: false },
             ticks: { font: { family: "'DM Sans', sans-serif" }, maxRotation: 45 } },
        y: { stacked: true, beginAtZero: true,
             grid: { color: '#e8e8e8', borderDash: [4, 4] },
             ticks: { font: { family: "'DM Sans', sans-serif" }, precision: 0 } }
      }
    }
  });

  function updateChart() {
    const { labels, datasets } = getChartData();
    chart.data.labels   = labels;
    chart.data.datasets = datasets;
    chart.update();
  }

  function togglePanel() { document.getElementById('panel').classList.toggle('open'); }
</script>
</body>
</html>"""

    html = (template
            .replace('%%CHARTJS%%', CHARTJS_CDN)
            .replace('%%DATA%%', data_json)
            .replace('%%GENERATED_AT%%', generated_at))

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)
    log.info(f"Chart written: {os.path.basename(filepath)}")


def write_scores_overview_chart_html(filepath, df, generated_at):
    """Filterable stacked bar chart of raw pressure scores per site with threshold line."""
    data_json = df.to_json(orient='records')

    template = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Pressure Management Scores Overview</title>
  <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
  <script src="%%CHARTJS%%"></script>
  <script src="%%ANNOTATION%%"></script>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    html,body{height:100%;overflow:hidden}
    body{font-family:'DM Sans',sans-serif;background:#fff}
    .wrap{background:#fff;padding:16px 20px 8px;position:relative;height:100%;display:flex;flex-direction:column}
    .chart-wrap{flex:1;min-height:0;position:relative}
    .hd{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:20px;gap:16px}
    .title{font-size:1rem;font-weight:600;color:#1a1a2e;line-height:1.4}
    .filter-btn{background:#273747;color:#fff;border:none;border-radius:6px;padding:7px 12px;cursor:pointer;display:flex;align-items:center;gap:6px;font-size:0.78rem;font-family:inherit;white-space:nowrap;flex-shrink:0}
    .filter-btn:hover{background:#3a4f63}
    .panel{display:none;position:absolute;right:24px;top:64px;background:#fff;border-radius:8px;box-shadow:0 4px 24px rgba(0,0,0,.18);z-index:100;min-width:280px;overflow:hidden}
    .panel.open{display:block}
    .panel-hd{background:#273747;color:#fff;padding:12px 16px;display:flex;align-items:center;justify-content:space-between}
    .panel-hd span{font-weight:600;font-size:0.9rem}
    .panel-close{background:none;border:none;color:#fff;cursor:pointer;font-size:1.1rem;line-height:1;opacity:.8}
    .panel-close:hover{opacity:1}
    .panel-body{padding:16px}
    .fg{margin-bottom:14px}
    .fg:last-child{margin-bottom:0}
    .fl{font-size:0.78rem;color:#555;font-weight:500;margin-bottom:6px}
    select{width:100%;border:1px solid #ddd;border-radius:6px;padding:8px 28px 8px 10px;font-size:0.85rem;color:#1a1a2e;font-family:inherit;appearance:none;background:#fff url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%23666' stroke-width='2.5'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E") no-repeat right 10px center;cursor:pointer}
    select:focus{outline:none;border-color:#273747}
    .updated{color:#aaa;font-size:.72em;margin-top:14px;text-align:right}
  </style>
</head>
<body>
<div class="wrap">
  <div class="hd">
    <div class="title">Pressure Management Scores Overview</div>
    <button class="filter-btn" onclick="togglePanel()">
      <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3"/></svg>
      Filter
    </button>
  </div>

  <div class="panel" id="panel">
    <div class="panel-hd">
      <span>Filter</span>
      <button class="panel-close" onclick="togglePanel()">&#x2715;</button>
    </div>
    <div class="panel-body">
      <div class="fg">
        <div class="fl">Financial Year is</div>
        <select id="fySelect" onchange="updateChart()"></select>
      </div>
      <div class="fg">
        <div class="fl">District is</div>
        <select id="regionSelect" onchange="updateChart()"></select>
      </div>
    </div>
  </div>

  <div class="chart-wrap"><canvas id="chart"></canvas></div>
  <p class="updated">Generated: %%GENERATED_AT%%</p>
</div>
<script>
  const rawData = %%DATA%%;
  const fys     = [...new Set(rawData.map(d => d.FY))].sort();
  const regions = [...new Set(rawData.map(d => d.Region).filter(r => r))].sort();

  const pressureDefs = [
    { label: 'Ungulates',      col: 'Weighted_Ungulates',     color: '#00695C' },
    { label: 'Pest Plants',    col: 'Weighted_PestPlants',    color: '#1565C0' },
    { label: 'Possum Browse',  col: 'Weighted_PossumBrowse',  color: '#43A047' },
    { label: 'Predation',      col: 'Weighted_Predation',     color: '#4DB6AC' },
    { label: 'Fragmentation',  col: 'Weighted_Environmental', color: '#8BC34A' },
    { label: 'Rabbits/Hares',  col: 'Weighted_Rabbits',       color: '#90CAF9' },
  ];

  const fySelect     = document.getElementById('fySelect');
  const regionSelect = document.getElementById('regionSelect');

  fys.forEach(fy => {
    const o = document.createElement('option');
    o.value = fy; o.textContent = fy;
    fySelect.appendChild(o);
  });
  fySelect.value = fys[fys.length - 1];

  [{ v: 'All Regions', t: 'All Regions' }, ...regions.map(r => ({ v: r, t: r }))].forEach(({ v, t }) => {
    const o = document.createElement('option');
    o.value = v; o.textContent = t;
    regionSelect.appendChild(o);
  });

  function getChartData() {
    const fy     = fySelect.value;
    const region = regionSelect.value;

    let rows = rawData.filter(d => String(d.FY) === String(fy));
    if (region !== 'All Regions') rows = rows.filter(d => d.Region === region);
    rows = rows.slice().sort((a, b) => String(a.SiteID).localeCompare(String(b.SiteID)));

    const labels = rows.map(d => d.SiteID);

    const datasets = pressureDefs.map(({ label, col, color }) => ({
      label,
      data: rows.map(d => Math.round((d[col] || 0) * 100) / 100),
      backgroundColor: color,
      stack: 'pressure'
    }));

    return { labels, datasets };
  }

  const { labels: initLabels, datasets: initDatasets } = getChartData();

  const chart = new Chart(document.getElementById('chart'), {
    type: 'bar',
    data: { labels: initLabels, datasets: initDatasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'bottom', labels: { font: { family: "'DM Sans', sans-serif" }, padding: 16 } },
        title:  { display: false },
        annotation: {
          annotations: {
            threshold: {
              type: 'line', yMin: 55, yMax: 55,
              borderColor: 'red', borderWidth: 2, borderDash: [6, 4],
              label: { display: true, content: 'Threshold (55)', position: 'end',
                       backgroundColor: 'red', color: '#fff', font: { size: 11 } }
            }
          }
        }
      },
      scales: {
        x: { stacked: true, grid: { display: false },
             ticks: { font: { family: "'DM Sans', sans-serif", size: 9 }, maxRotation: 90 } },
        y: { stacked: true, min: 0, max: 100,
             grid: { color: '#e8e8e8', borderDash: [4, 4] },
             ticks: { font: { family: "'DM Sans', sans-serif" } } }
      }
    }
  });

  function updateChart() {
    const { labels, datasets } = getChartData();
    chart.data.labels   = labels;
    chart.data.datasets = datasets;
    chart.update();
  }

  function togglePanel() { document.getElementById('panel').classList.toggle('open'); }
</script>
</body>
</html>"""

    html = (template
            .replace('%%CHARTJS%%', CHARTJS_CDN)
            .replace('%%ANNOTATION%%', ANNOTATION_CDN)
            .replace('%%DATA%%', data_json)
            .replace('%%GENERATED_AT%%', generated_at))

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html)
    log.info(f"Chart written: {os.path.basename(filepath)}")


# --- Chart 1: Pressure Scores Overview (filterable by FY and District) ---

chart1_cols = ['SiteID', 'FY', 'Region',
               'Weighted_Ungulates', 'Weighted_PestPlants', 'Weighted_PossumBrowse',
               'Weighted_Predation', 'Weighted_Environmental', 'Weighted_Rabbits']

write_scores_overview_chart_html(
    os.path.join(CHARTS_DIR, 'PH_Pressure_Scores_Overview.html'),
    related_table_data[chart1_cols],
    generated_at
)

# --- Chart 2: Average Pressure by Ecosystem Type (filterable by FY and District) ---

write_ecosystem_chart_html(
    os.path.join(CHARTS_DIR, 'PH_Pressure_by_Ecosystem.html'),
    pressure_by_ecosystem,
    generated_at
)

# --- Chart 3: Sites Above/Below Threshold by District & Ecosystem (filterable by FY and District) ---

write_threshold_chart_html(
    os.path.join(CHARTS_DIR, 'PH_Sites_Threshold_Status.html'),
    threshold_by_region,
    generated_at
)

log.info(f"All charts written to: {CHARTS_DIR}")

# ============================================================
# COMPLETE
# ============================================================

log.info("=" * 70)
log.info("PROCESS COMPLETE")
log.info("=" * 70)
log.info("OUTPUTS:")
log.info(f"  Main Feature Class : {main_fc_path} ({main_fc_count} records)")
log.info(f"  Related Table      : {related_table_path} ({related_table_count} rows)")
log.info(f"  Summary CSV        : {OUTPUT_SUMMARY_CSV}")
log.info(f"  HTML Charts        : {CHARTS_DIR}")
log.info(f"  Log                : {log_file}")
log.info(f"  Timestamp          : {dt.now().strftime('%Y-%m-%d %H:%M:%S')}")
log.info("=" * 70)
