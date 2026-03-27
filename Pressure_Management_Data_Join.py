import pandas as pd
import numpy as np
from arcgis.features import FeatureLayer
import arcpy
import os
import json
from datetime import datetime
from config import CSV_PATH, OUTPUT_GDB, NETWORK_DIR, OUTPUT_SUMMARY_CSV

print("Libraries imported successfully")

# ArcGIS Server REST Endpoint
FEATURE_SERVICE_URL = "https://services1.arcgis.com/VuN78wcRdq1Oj69W/arcgis/rest/services/Priority_habitats_with_PCO/FeatureServer/0"

OUTPUT_MAIN_FC = "PH_Pressure_Management"
OUTPUT_RELATED_TABLE = "PH_Pressure_Scores_TimeSeries"

print(f"Feature Service URL: {FEATURE_SERVICE_URL}")
print(f"CSV Path: {CSV_PATH}")
print(f"Output GDB: {OUTPUT_GDB}")
print(f"Output Main FC: {OUTPUT_MAIN_FC}")
print(f"Output Related Table: {OUTPUT_RELATED_TABLE}")
print(f"Output Summary CSV: {OUTPUT_SUMMARY_CSV}")
print(f"\nPaths configured")

from arcgis.gis import GIS

# Connect to feature service using Pro portal credentials
print("Connecting to feature service using ArcGIS Pro sign-in...")

gis = GIS("home")
feature_layer = FeatureLayer(FEATURE_SERVICE_URL, gis=gis)

print("Connected to feature service")

# Query all features
print("\nQuerying all features...")
features = feature_layer.query(
    where="1=1",
    out_fields="*",
    return_geometry=True
)

spatial_data = features.sdf

print(f"Retrieved {len(spatial_data)} features")
print(f"\nSpatial data shape: {spatial_data.shape}")
print("First few rows:")
print(spatial_data[['SiteID', 'SiteName', 'Actual_EcoSystem_s_', 'AreaHa']].head())


# Load local CSV (manual export from SharePoint - Raw site data tab)
print("Loading pressure data CSV...")
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

print(f"Pressure data shape: {pressure_data.shape}")
print(f"\nFirst few rows:")
print(pressure_data[['SiteID', 'SiteName', 'FY', 'Total_Score', 'Above_55_Threshold']].head(10))
print(f"\nFinancial Years in data: {sorted(pressure_data['FY'].unique())}")

print("\n" + "="*70)
print("JOIN VALIDATION")
print("="*70)

spatial_ids = set(spatial_data['SiteID'].dropna())
pressure_ids = set(pressure_data['SiteID'].dropna())

print(f"\nSpatial sites: {len(spatial_ids)}")
print(f"Pressure sites: {len(pressure_ids)}")

matched = spatial_ids.intersection(pressure_ids)
unmatched_spatial = spatial_ids - pressure_ids
unmatched_pressure = pressure_ids - spatial_ids

print(f"\nMatched: {len(matched)}")

if unmatched_spatial:
    print(f"\nSpatial sites NOT in pressure data ({len(unmatched_spatial)}):")
    print(f"  {sorted(unmatched_spatial)}")

if unmatched_pressure:
    print(f"\nPressure sites NOT in spatial data ({len(unmatched_pressure)}):")
    print(f"  {sorted(unmatched_pressure)}")

if not unmatched_spatial and not unmatched_pressure:
    print(f"\nPerfect match! All sites can be joined successfully.")

print(f"\nMatch rate: {len(matched)} / {len(spatial_ids)} = {100*len(matched)/len(spatial_ids):.1f}%")

# Main feature class - spatial data only
main_fc_data = spatial_data[[
    'SiteID', 'SiteName', 'Actual_EcoSystem_s_', 'AreaHa', 
    'HRCLevel', 'HRCStaff', 'Management', 'Protection',
    'SHAPE'
]].copy()

main_fc_data = main_fc_data.rename(columns={
    'Actual_EcoSystem_s_': 'Ecosystem'
})

# Join latest Total_Score and threshold status to main feature class
print("\nAdding latest pressure scores to main feature class...")

# Get latest FY per site from pressure data
latest_scores = pressure_data.sort_values('FY').drop_duplicates(subset=['SiteID'], keep='last')[['SiteID', 'Total_Score', 'Above_55_Threshold', 'FY']]

# Merge with main feature class
main_fc_data = main_fc_data.merge(latest_scores, on='SiteID', how='left')

print(f"Added columns: Total_Score, Above_55_Threshold, FY")
print(f"\nMain feature class with scores:\")")
print(main_fc_data[['SiteID', 'SiteName', 'Total_Score', 'Above_55_Threshold']].head(10))

print(f"Main feature class data:")
print(f"  Shape: {main_fc_data.shape}")
print(f"  Unique sites: {main_fc_data['SiteID'].nunique()}")
print(f"\nFirst few rows:")
print(main_fc_data[['SiteID', 'SiteName', 'Ecosystem', 'AreaHa', 'Total_Score', 'Above_55_Threshold', 'FY']].head())

# Related table - one row per site per financial year
related_table_data = pressure_data[[
    'SiteID', 'FY', 'Region', 'Ecosystem', 
    'Raw_Ungulates', 'Raw_PestPlants', 'Raw_PossumBrowse', 'Raw_Predation', 
    'Raw_Environmental', 'Raw_Rabbits',
    'Weighted_Ungulates', 'Weighted_PestPlants', 'Weighted_PossumBrowse', 
    'Weighted_Predation', 'Weighted_Environmental', 'Weighted_Rabbits',
    'Total_Score', 'Above_55_Threshold', 'Lead'
]].copy()

print(f"Related table data:")
print(f"  Shape: {related_table_data.shape}")
print(f"  Unique sites: {related_table_data['SiteID'].nunique()}")
print(f"  Financial years: {sorted(related_table_data['FY'].unique())}")
print(f"\nFirst few rows:")
print(related_table_data[['SiteID', 'FY', 'Total_Score', 'Above_55_Threshold']].head(10))

# Summary table - breakdown by District AND Financial Year
print("Creating summary by District and Financial Year...")

# Group by Region (District) and FY
summary = pressure_data.groupby(['Region', 'FY']).agg({
    'SiteID': 'count',
    'Total_Score': 'mean',
    'Above_55_Threshold': 'sum'  # using >55 threshold
}).reset_index()

summary.columns = ['District', 'Financial_Year', 'No_of_Sites', 'Average_Score', 'Sites_Above_Threshold']

# Calculate sites below threshold
summary['Sites_Below_Threshold'] = summary['No_of_Sites'] - summary['Sites_Above_Threshold']

# Calculate percentages
summary['Pct_Above_Threshold'] = (summary['Sites_Above_Threshold'] / summary['No_of_Sites'] * 100).round(1)
summary['Pct_Below_Threshold'] = (summary['Sites_Below_Threshold'] / summary['No_of_Sites'] * 100).round(1)

# Round Average_Score
summary['Average_Score'] = summary['Average_Score'].round(2)

# Reorder columns
summary = summary[[
    'District', 'Financial_Year', 'No_of_Sites', 'Average_Score', 'Sites_Above_Threshold', 'Sites_Below_Threshold', 'Pct_Above_Threshold', 'Pct_Below_Threshold'
]].copy()

# Sort by District and FY
summary = summary.sort_values(['District', 'Financial_Year']).reset_index(drop=True)

print(f"\nSummary by District and Financial Year:")
print(summary.to_string(index=False))

## Step 8.5: Create Pressure by Ecosystem Summary Table

print("\nCreating pressure by ecosystem and financial year summary...")

pressure_mapping = {
    'Raw_Ungulates': 'Ungulates',
    'Raw_PestPlants': 'Pest plants',
    'Raw_PossumBrowse': 'Possum browse',
    'Raw_Predation': 'Predation',
    'Raw_Environmental': 'Environmental disturbance',
    'Raw_Rabbits': 'Rabbits/hares'
}

pressure_ecosystem_data = []

# 1. Whole region averages (all regions + all ecosystems)
for ecosystem in sorted(pressure_data['Ecosystem'].unique()):
    for fy in sorted(pressure_data['FY'].unique()):
        eco_fy_data = pressure_data[(pressure_data['Ecosystem'] == ecosystem) & (pressure_data['FY'] == fy)]
        if len(eco_fy_data) > 0:
            for raw_col, pressure_name in pressure_mapping.items():
                avg_score = eco_fy_data[raw_col].mean()
                pressure_ecosystem_data.append({
                    'Region': 'All Regions',
                    'Ecosystem': ecosystem,
                    'Financial_Year': fy,
                    'Pressure_Type': pressure_name,
                    'Average_Score': round(avg_score, 2)
                })

# 2. Regional averages (by specific region + ecosystem + fy)
for region in sorted(pressure_data['Region'].unique()):
    for ecosystem in sorted(pressure_data['Ecosystem'].unique()):
        for fy in sorted(pressure_data['FY'].unique()):
            region_eco_fy_data = pressure_data[(pressure_data['Region'] == region) & 
                                               (pressure_data['Ecosystem'] == ecosystem) & 
                                               (pressure_data['FY'] == fy)]
            if len(region_eco_fy_data) > 0:
                for raw_col, pressure_name in pressure_mapping.items():
                    avg_score = region_eco_fy_data[raw_col].mean()
                    pressure_ecosystem_data.append({
                        'Region': region,
                        'Ecosystem': ecosystem,
                        'Financial_Year': fy,
                        'Pressure_Type': pressure_name,
                        'Average_Score': round(avg_score, 2)
                    })

pressure_by_ecosystem = pd.DataFrame(pressure_ecosystem_data)

# Add concatenated Region_Ecosystem field
pressure_by_ecosystem['Region_Ecosystem'] = pressure_by_ecosystem['Region'] + ' ' + pressure_by_ecosystem['Ecosystem']

# Reorder columns
pressure_by_ecosystem = pressure_by_ecosystem[[
    'Region_Ecosystem', 'Region', 'Ecosystem', 'Financial_Year', 'Pressure_Type', 'Average_Score'
]].copy()

print(f"\nPressure Management by Region, Ecosystem and Financial Year:")
print(pressure_by_ecosystem.to_string(index=False))

# Export to CSV
pressure_ecosystem_csv = os.path.join(NETWORK_DIR, "PH_Pressure_by_Ecosystem.csv")
pressure_by_ecosystem.to_csv(pressure_ecosystem_csv, index=False)
print(f"\nSaved to: {pressure_ecosystem_csv}")

## Step 8.6: Create Sites Above/Below Threshold by Region, Ecosystem and Financial Year

print("\nCreating sites above/below threshold by region, ecosystem and FY...")

threshold_data = []

for region in sorted(pressure_data['Region'].unique()):
    for ecosystem in sorted(pressure_data['Ecosystem'].unique()):
        for fy in sorted(pressure_data['FY'].unique()):
            region_eco_fy_data = pressure_data[(pressure_data['Region'] == region) & 
                                               (pressure_data['Ecosystem'] == ecosystem) & 
                                               (pressure_data['FY'] == fy)]
            if len(region_eco_fy_data) > 0:
                sites_above = int(region_eco_fy_data['Above_55_Threshold'].sum())
                sites_below = len(region_eco_fy_data) - sites_above
                region_ecosystem = f"{ecosystem} {region}"
                
                # Above threshold row
                threshold_data.append({
                    'Region_Ecosystem': region_ecosystem,
                    'Region': region,
                    'Ecosystem': ecosystem,
                    'Financial_Year': fy,
                    'Threshold_Status': 'Above',
                    'Count': sites_above
                })
                
                # Below threshold row
                threshold_data.append({
                    'Region_Ecosystem': region_ecosystem,
                    'Region': region,
                    'Ecosystem': ecosystem,
                    'Financial_Year': fy,
                    'Threshold_Status': 'Below',
                    'Count': sites_below
                })

threshold_by_region = pd.DataFrame(threshold_data)

print(f"\nSites Above/Below Threshold by Region, Ecosystem and Financial Year:")
print(threshold_by_region.to_string(index=False))

# Export to CSV
threshold_csv = os.path.join(NETWORK_DIR, "PH_Sites_Threshold_Status.csv")
threshold_by_region.to_csv(threshold_csv, index=False)
print(f"\nSaved to: {threshold_csv}")

# Set up ArcPy workspace
arcpy.env.workspace = OUTPUT_GDB
arcpy.env.overwriteOutput = True

print(f"Setting ArcPy workspace to: {OUTPUT_GDB}")
print(f"Overwrite existing: True")

if not os.path.exists(OUTPUT_GDB):
    print(f"\nCreating geodatabase...")
    arcpy.CreateFileGDB_management(os.path.dirname(OUTPUT_GDB), os.path.basename(OUTPUT_GDB))
    print("Geodatabase created")
else:
    print(f"Geodatabase exists")

# Export Main Feature Class
print(f"\nExporting main feature class: {OUTPUT_MAIN_FC}")
main_fc_path = os.path.join(OUTPUT_GDB, OUTPUT_MAIN_FC)
main_fc_data.spatial.to_featureclass(main_fc_path, overwrite=True)
print(f"Main feature class created")

# Export Related Table
print(f"\nExporting related table: {OUTPUT_RELATED_TABLE}")
related_table_path = os.path.join(OUTPUT_GDB, OUTPUT_RELATED_TABLE)
temp_csv = os.path.join(OUTPUT_GDB, OUTPUT_RELATED_TABLE + '.csv')
related_table_data.to_csv(temp_csv, index=False)
arcpy.conversion.TableToTable(temp_csv, OUTPUT_GDB, OUTPUT_RELATED_TABLE)
if os.path.exists(temp_csv):
    os.remove(temp_csv)
print(f"Related table created")

# Export Summary CSV
print(f"\nExporting summary table to CSV")
summary.to_csv(OUTPUT_SUMMARY_CSV, index=False)
print(f"Summary CSV created")

print("\n" + "="*70)
print("VALIDATION")
print("="*70)

# Check main feature class
main_fc_count = int(arcpy.management.GetCount(main_fc_path)[0])
print(f"\nMain Feature Class: {OUTPUT_MAIN_FC}")
print(f"  Features: {main_fc_count}")

# Check related table
related_table_count = int(arcpy.management.GetCount(related_table_path)[0])
print(f"\nRelated Table: {OUTPUT_RELATED_TABLE}")
print(f"  Rows: {related_table_count}")
print(f"  Fields: {[f.name for f in arcpy.ListFields(related_table_path) if not f.name.startswith('OID')]}")

# Check summary CSV
print(f"\nSummary Table")
print(f"  Rows: {len(summary)}")
print(f"\n{summary.to_string(index=False)}")

print(f"\n" + "="*70)
print("ALL EXPORTS COMPLETE")
print("="*70)

print("\n" + "="*70)
print("PROCESS COMPLETE")
print("="*70)
print(f"\nOUTPUTS CREATED:")
print(f"\n1. MAIN FEATURE CLASS")
print(f"   Path: {main_fc_path}")
print(f"   Records: {main_fc_count}")
print(f"   Purpose: Unique sites for spatial mapping")
print(f"\n2. RELATED TABLE")
print(f"   Path: {related_table_path}")
print(f"   Records: {related_table_count}")
print(f"   Purpose: Time-series pressure scores")
print(f"\n3. SUMMARY CSV")
print(f"   Path: {OUTPUT_SUMMARY_CSV}")
print(f"   Districts: {len(summary)}")
print(f"   Purpose: District-level statistics")
print(f"\nTIMESTAMP: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("\n" + "="*70)

# Create relationship between main feature class and related table
print("\nCreating relationship between feature class and related table...")

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

print(f"Relationship created successfully!")
print(f"  Origin: {OUTPUT_MAIN_FC}")
print(f"  Destination: {OUTPUT_RELATED_TABLE}")
print(f"  Relationship: SiteID to SiteID (1 to Many)")


# ============================================================
# GENERATE HTML CHART FILES
# ============================================================

print("\n" + "="*70)
print("GENERATING HTML CHARTS")
print("="*70)

CHARTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "html", "pressure-management")
os.makedirs(CHARTS_DIR, exist_ok=True)

generated_at = datetime.now().strftime('%Y-%m-%d %H:%M')
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
    print(f"  Created: {filepath}")


# --- Chart 1: Pressure Scores Overview (latest FY per site) ---

chart1_df = (related_table_data
             .sort_values('FY')
             .drop_duplicates(subset=['SiteID'], keep='last')
             .sort_values('SiteID'))

pressure_defs = [
    ('Ungulates',                 'Raw_Ungulates',     '#00695C'),
    ('Pest Plants',               'Raw_PestPlants',    '#1565C0'),
    ('Possum Browse',             'Raw_PossumBrowse',  '#43A047'),
    ('Predation',                 'Raw_Predation',     '#4DB6AC'),
    ('Environmental Disturbance', 'Raw_Environmental', '#8BC34A'),
    ('Rabbits/Hares',             'Raw_Rabbits',       '#90CAF9'),
]

datasets1 = [
    {
        'label': label,
        'data': [round(float(v), 2) if pd.notna(v) else 0 for v in chart1_df[col]],
        'backgroundColor': color,
        'stack': 'pressure',
    }
    for label, col, color in pressure_defs
]

config1 = {
    'type': 'bar',
    'data': {'labels': chart1_df['SiteID'].tolist(), 'datasets': datasets1},
    'options': {
        'responsive': True,
        'plugins': {
            'title': {'display': True, 'text': 'Pressure Management Scores Overview', 'font': {'size': 16}},
            'legend': {'position': 'bottom'},
            'annotation': {
                'annotations': {
                    'threshold': {
                        'type': 'line', 'yMin': 55, 'yMax': 55,
                        'borderColor': 'red', 'borderWidth': 2,
                        'label': {'display': True, 'content': 'Threshold (55)', 'position': 'end',
                                  'backgroundColor': 'red', 'color': 'white', 'font': {'size': 11}}
                    }
                }
            }
        },
        'scales': {
            'x': {'stacked': True, 'ticks': {'maxRotation': 90, 'font': {'size': 9}}},
            'y': {'stacked': True, 'min': 0, 'max': 100, 'title': {'display': True, 'text': 'Score'}}
        }
    }
}

write_chart_html(
    os.path.join(CHARTS_DIR, 'PH_Pressure_Scores_Overview.html'),
    'Pressure Management Scores Overview',
    json.dumps(config1)
)


# --- Chart 2: Average Pressure by Ecosystem Type (median across FYs, All Regions) ---

chart2_df = (pressure_by_ecosystem[pressure_by_ecosystem['Region'] == 'All Regions']
             .groupby(['Ecosystem', 'Pressure_Type'])['Average_Score']
             .median()
             .reset_index())

pressure_types2  = sorted(chart2_df['Pressure_Type'].unique())
ecosystems2      = sorted(chart2_df['Ecosystem'].unique())
eco_colors       = {'Coastal': '#F9A825', 'Forest': '#2E7D32', 'Wetland': '#1565C0'}

datasets2 = []
for eco in ecosystems2:
    eco_pivot = chart2_df[chart2_df['Ecosystem'] == eco].set_index('Pressure_Type')
    datasets2.append({
        'label': eco,
        'data': [round(float(eco_pivot.loc[pt, 'Average_Score']), 2) if pt in eco_pivot.index else 0
                 for pt in pressure_types2],
        'backgroundColor': eco_colors.get(eco, '#888888')
    })

config2 = {
    'type': 'bar',
    'data': {'labels': pressure_types2, 'datasets': datasets2},
    'options': {
        'responsive': True,
        'plugins': {
            'title': {'display': True, 'text': 'Average Pressure Management Score by Ecosystem Type', 'font': {'size': 16}},
            'legend': {'position': 'bottom'}
        },
        'scales': {
            'x': {'stacked': False},
            'y': {'min': 0, 'max': 100, 'title': {'display': True, 'text': 'Median Score'}}
        }
    }
}

write_chart_html(
    os.path.join(CHARTS_DIR, 'PH_Pressure_by_Ecosystem.html'),
    'Average Pressure by Ecosystem Type',
    json.dumps(config2)
)


# --- Chart 3: Sites Above/Below Threshold by District & Ecosystem (all FYs summed) ---

chart3_df = (threshold_by_region
             .groupby(['Region_Ecosystem', 'Threshold_Status'])['Count']
             .sum()
             .reset_index())

region_eco_labels = sorted(chart3_df['Region_Ecosystem'].unique())
threshold_colors  = {'Above': '#43A047', 'Below': '#1565C0'}

datasets3 = []
for status in ['Below', 'Above']:
    status_pivot = chart3_df[chart3_df['Threshold_Status'] == status].set_index('Region_Ecosystem')
    datasets3.append({
        'label': f'{status} Threshold',
        'data': [int(status_pivot.loc[r, 'Count']) if r in status_pivot.index else 0
                 for r in region_eco_labels],
        'backgroundColor': threshold_colors[status],
        'stack': 'threshold'
    })

config3 = {
    'type': 'bar',
    'data': {'labels': region_eco_labels, 'datasets': datasets3},
    'options': {
        'responsive': True,
        'plugins': {
            'title': {'display': True, 'text': 'No. of Sites Above/Below Threshold by District & Ecosystem Type', 'font': {'size': 16}},
            'legend': {'position': 'bottom'}
        },
        'scales': {
            'x': {'stacked': True, 'ticks': {'maxRotation': 45}},
            'y': {'stacked': True, 'beginAtZero': True, 'title': {'display': True, 'text': 'No. of Sites'}}
        }
    }
}

write_chart_html(
    os.path.join(CHARTS_DIR, 'PH_Sites_Threshold_Status.html'),
    'Sites Above/Below Threshold by District & Ecosystem',
    json.dumps(config3)
)

print(f"\nAll charts written to: {CHARTS_DIR}")


