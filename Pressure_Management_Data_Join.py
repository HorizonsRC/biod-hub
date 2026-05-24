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
from arcgis.features import FeatureLayer, FeatureLayerCollection, FeatureSet
import arcpy
from config import CSV_PATH, OUTPUT_GDB, NETWORK_DIR, OUTPUT_SUMMARY_CSV, FEATURE_SERVICE_URL

log.info("Libraries imported successfully")

# FEATURE_SERVICE_URL is read from config.py

OUTPUT_MAIN_FC = "PH_Pressure_Management"
OUTPUT_RELATED_TABLE = "PH_Pressure_Scores_TimeSeries"

# AGOL push — target hosted feature service (Priority_Habitats_Pressure_Management)
PUSH_TO_AGOL = True
AGOL_TARGET_ITEM_ID = "1333a0f1cadb42de8a708bbdb2bf6b67"

# main_fc_data column -> AGOL layer 0 (PH_Pressure_Management) field name.
# Layer 0 fields were lower-cased when published; District/site_programme were not.
LAYER0_FIELD_MAP = {
    'SiteID': 'site_id', 'SiteName': 'site_name', 'Ecosystem': 'ecosystem',
    'AreaHa': 'area_ha', 'HRCLevel': 'hrc_level', 'HRCStaff': 'hrc_staff',
    'Management': 'management', 'Protection': 'protection',
    'Total_Score': 'total_score', 'Above_55_Threshold': 'above_55_threshold',
    'FY': 'fy', 'District': 'District', 'site_programme': 'site_programme',
}
# The related table kept its original field names — columns map 1:1 to AGOL.

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

# Hardcoded site programme classification (not in spreadsheet)
_PROGRAMME_MAP = {
    'Palm05':  'Icon Site',
    'Horo34W': 'Icon Site',
    'Whan35':  'Icon Site',
    'Man240':  'Regional Park',
}
main_fc_data['site_programme'] = main_fc_data['SiteID'].map(_PROGRAMME_MAP).fillna('Priority Habitat')
log.info(f"Site programme counts:\n{main_fc_data['site_programme'].value_counts().to_string()}")

log.info("Adding latest pressure scores to main feature class...")

latest_scores = pressure_data.sort_values('FY').drop_duplicates(subset=['SiteID'], keep='last')[
    ['SiteID', 'Total_Score', 'Above_55_Threshold', 'FY', 'Region']
]

main_fc_data = main_fc_data.merge(latest_scores, on='SiteID', how='left')

# AGOL layer 0 calls the region/district code 'District' — rename to match
main_fc_data = main_fc_data.rename(columns={'Region': 'District'})

# A site can exist in the spatial layer before it has any pressure scores.
# Treat an unscored site as 0 (below threshold) so it still appears and pushes
# cleanly — leaving the merged values as NaN flips the column to float / null
# and AGOL rejects the whole batch (rollback, error 1003).
unscored = main_fc_data['Total_Score'].isna()
if unscored.any():
    latest_fy = sorted(pressure_data['FY'].dropna().unique())[-1]
    log.info(f"Sites with no pressure data ({int(unscored.sum())}) — defaulting "
             f"score to 0, FY to {latest_fy}: "
             f"{sorted(main_fc_data.loc[unscored, 'SiteID'])}")
    main_fc_data['Total_Score'] = main_fc_data['Total_Score'].fillna(0)
    main_fc_data['FY'] = main_fc_data['FY'].fillna(latest_fy)
    main_fc_data['District'] = main_fc_data['District'].fillna(
        main_fc_data['SiteID'].str.extract(r'^([A-Za-z]+)', expand=False))
# fillna + astype keeps this a clean int column (a NaN would force it to float)
main_fc_data['Above_55_Threshold'] = main_fc_data['Above_55_Threshold'].fillna(0).astype(int)

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

related_table_data['site_programme'] = related_table_data['SiteID'].map(_PROGRAMME_MAP).fillna('Priority Habitat')

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

# Aggregations are produced for two scopes — every site, and Priority Habitat
# only (Icon Sites + the Regional Park excluded; they skew the averages).
priority_data = pressure_data[~pressure_data['SiteID'].isin(list(_PROGRAMME_MAP))]
SCOPES = [('All Sites', pressure_data), ('Priority Habitat', priority_data)]


def build_ecosystem_rows(df, scope):
    rows = []
    groups = [('All Regions', df)] + [
        (r, df[df['Region'] == r]) for r in sorted(df['Region'].dropna().unique())
    ]
    for region, region_df in groups:
        for ecosystem in sorted(region_df['Ecosystem'].unique()):
            for fy in sorted(region_df['FY'].unique()):
                cell = region_df[(region_df['Ecosystem'] == ecosystem) & (region_df['FY'] == fy)]
                if len(cell) == 0:
                    continue
                for raw_col, pressure_name in pressure_mapping.items():
                    rows.append({
                        'Scope': scope, 'Region': region, 'Ecosystem': ecosystem,
                        'Financial_Year': fy, 'Pressure_Type': pressure_name,
                        'Average_Score': round(cell[raw_col].mean(), 2),
                    })
    return rows


pressure_ecosystem_data = []
for scope_label, scope_df in SCOPES:
    pressure_ecosystem_data += build_ecosystem_rows(scope_df, scope_label)

pressure_by_ecosystem = pd.DataFrame(pressure_ecosystem_data)
pressure_by_ecosystem['Region_Ecosystem'] = pressure_by_ecosystem['Region'] + ' ' + pressure_by_ecosystem['Ecosystem']
pressure_by_ecosystem = pressure_by_ecosystem[[
    'Scope', 'Region_Ecosystem', 'Region', 'Ecosystem', 'Financial_Year', 'Pressure_Type', 'Average_Score'
]].copy()

log.info(f"Pressure by Ecosystem:\n{pressure_by_ecosystem.to_string(index=False)}")

pressure_ecosystem_csv = os.path.join(NETWORK_DIR, "PH_Pressure_by_Ecosystem.csv")
pressure_by_ecosystem.to_csv(pressure_ecosystem_csv, index=False)
log.info(f"Saved: {pressure_ecosystem_csv}")

# Sites Above/Below Threshold by Region, Ecosystem and Financial Year
log.info("Creating sites above/below threshold by region, ecosystem and FY...")

def build_threshold_rows(df, scope):
    rows = []
    for region in sorted(df['Region'].dropna().unique()):
        for ecosystem in sorted(df['Ecosystem'].unique()):
            for fy in sorted(df['FY'].unique()):
                cell = df[(df['Region'] == region) &
                          (df['Ecosystem'] == ecosystem) &
                          (df['FY'] == fy)]
                if len(cell) == 0:
                    continue
                above = int(cell['Above_55_Threshold'].sum())
                below = len(cell) - above
                re_label = f"{ecosystem} {region}"
                for status, count in [('Above', above), ('Below', below)]:
                    rows.append({
                        'Scope': scope, 'Region_Ecosystem': re_label, 'Region': region,
                        'Ecosystem': ecosystem, 'Financial_Year': fy,
                        'Threshold_Status': status, 'Count': count,
                    })
    return rows


threshold_data = []
for scope_label, scope_df in SCOPES:
    threshold_data += build_threshold_rows(scope_df, scope_label)

threshold_by_region = pd.DataFrame(threshold_data)

log.info(f"Sites Above/Below Threshold:\n{threshold_by_region.to_string(index=False)}")

threshold_csv = os.path.join(NETWORK_DIR, "PH_Sites_Threshold_Status.csv")
threshold_by_region.to_csv(threshold_csv, index=False)
log.info(f"Saved: {threshold_csv}")

scores_overview_cols = ['SiteID', 'FY', 'Region', 'site_programme',
                        'Raw_Ungulates', 'Raw_PestPlants', 'Raw_PossumBrowse',
                        'Raw_Predation', 'Raw_Environmental', 'Raw_Rabbits',
                        'Weighted_Ungulates', 'Weighted_PestPlants', 'Weighted_PossumBrowse',
                        'Weighted_Predation', 'Weighted_Environmental', 'Weighted_Rabbits']
scores_csv = os.path.join(NETWORK_DIR, "PH_Pressure_Scores_Overview.csv")
related_table_data[scores_overview_cols].to_csv(scores_csv, index=False)
log.info(f"Saved: {scores_csv}")

# Per-site programme list for ALL spatial sites — used by the dashboard intro
# count so unscored sites (absent from the scores-based CSVs by design) still
# get counted in the per-programme totals.
sites_programme_csv = os.path.join(NETWORK_DIR, "PH_Sites_Programme.csv")
main_fc_data[['SiteID', 'site_programme']].to_csv(sites_programme_csv, index=False)
log.info(f"Saved: {sites_programme_csv}")

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
# PUSH TO AGOL  (delete-all + add — schema unchanged, safe with the joined view)
# ============================================================

if PUSH_TO_AGOL:
    import math

    log.info("=" * 70)
    log.info("PUSHING DATA TO AGOL")
    log.info("=" * 70)

    def _clean(value):
        # Normalise for JSON: NaN -> None, numpy scalars -> native python
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        if hasattr(value, "item"):
            return value.item()
        return value

    target_item = gis.content.get(AGOL_TARGET_ITEM_ID)
    target_flc = FeatureLayerCollection.fromitem(target_item)
    agol_layer = target_flc.layers[0]   # PH_Pressure_Management
    agol_table = target_flc.tables[0]   # PH_Pressure_Scores
    log.info(f"Target service: {target_item.title} ({AGOL_TARGET_ITEM_ID})")

    def replace_features(target, features):
        name = target.properties.name
        existing = target.query(where="1=1", return_ids_only=True)["objectIds"]
        log.info(f"  [{name}] replacing {len(existing)} rows with {len(features)}...")
        result = target.edit_features(
            adds=features,
            deletes=existing if existing else None,
            rollback_on_failure=True,
        )
        added   = sum(1 for r in result.get("addResults", []) if r.get("success"))
        deleted = sum(1 for r in result.get("deleteResults", []) if r.get("success"))
        log.info(f"  [{name}] deleted {deleted}, added {added}")
        if added != len(features):
            # rollback_on_failure=True makes one bad row roll the whole batch back,
            # and AGOL then reports the generic code 1003 for *every* row — the real
            # cause is hidden. Re-run the adds with rollback OFF so the genuine
            # per-feature errors surface, then delete the test rows so the layer is
            # left exactly as it was.
            log.error(f"  [{name}] batch rolled back — retrying with rollback OFF "
                      f"to surface the real per-feature errors...")
            diag = target.edit_features(adds=features, rollback_on_failure=False)
            diag_results = diag.get("addResults", [])
            real_fails = [(i, r) for i, r in enumerate(diag_results)
                          if not r.get("success")]
            for i, r in real_fails:
                attrs = features[i].attributes
                sid = attrs.get("site_id") or attrs.get("SiteID") or "<no SiteID>"
                log.error(f"    [{name}] FAILED site={sid}  error={r.get('error')}")
            added_ids = [r["objectId"] for r in diag_results if r.get("success")]
            if added_ids:
                target.edit_features(deletes=added_ids, rollback_on_failure=False)
                log.info(f"  [{name}] removed {len(added_ids)} diagnostic test rows")
            raise RuntimeError(
                f"{name}: {len(real_fails)}/{len(features)} feature(s) genuinely "
                f"failed — see the 'FAILED site=' lines above for the real error.")

    # Layer 0 features (with geometry) — remap field names, clean values
    layer_fs = main_fc_data.spatial.to_featureset()
    for feat in layer_fs.features:
        feat.attributes = {LAYER0_FIELD_MAP[k]: _clean(v)
                           for k, v in feat.attributes.items() if k in LAYER0_FIELD_MAP}

    # Table features (no geometry) — columns already match AGOL field names
    table_fs = FeatureSet.from_dataframe(related_table_data)
    for feat in table_fs.features:
        feat.attributes = {k: _clean(v) for k, v in feat.attributes.items()}

    # The service is published Query-only — editing must be on for applyEdits.
    original_caps = target_flc.properties.capabilities
    log.info(f"Capabilities before: {original_caps}")
    try:
        # Static-data layers reject editing — clear the flag first, then enable editing
        target_flc.manager.update_definition({"hasStaticData": False})
        target_flc.manager.update_definition(
            {"capabilities": "Query,Create,Update,Delete"})
        log.info("Editing enabled — pushing features...")
        replace_features(agol_layer, layer_fs.features)
        replace_features(agol_table, table_fs.features)
    finally:
        target_flc.manager.update_definition({"capabilities": original_caps})
        log.info(f"Capabilities restored to: {original_caps}")

    log.info("AGOL push complete")
else:
    log.info("PUSH_TO_AGOL is False — AGOL not updated")

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
log.info(f"  Log                : {log_file}")
log.info(f"  Timestamp          : {dt.now().strftime('%Y-%m-%d %H:%M:%S')}")
log.info("=" * 70)
