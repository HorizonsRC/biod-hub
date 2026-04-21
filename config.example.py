# config.example.py — copy this to config.py and fill in your local paths

# Local path to the manually exported SharePoint CSV (Raw site data tab)
CSV_PATH = r"C:\path\to\your\Data\Pressure Management Reporting System Data 2025(Raw site data).csv"

# Output file geodatabase
OUTPUT_GDB = r"\\your-server\your-share\PH_Pressure_Management\PH_Pressure_Management.gdb"

# Network directory for CSV outputs — read by both scripts
# (PH_Pressure_by_Ecosystem.csv, PH_Sites_Threshold_Status.csv, PH_Pressure_Scores_Overview.csv)
NETWORK_DIR = r"\\your-server\your-share\PH_Pressure_Management"

# Local path for district summary CSV
OUTPUT_SUMMARY_CSV = r"C:\path\to\your\Outputs\PH_Pressure_Summary_by_District.csv"

# ── Icon Sites ────────────────────────────────────────────────────────────────

# Output directory for per-site summary CSVs (created automatically if absent)
# Recommended: point this to your local Data\ folder (gitignored)
ICON_SITES_OUTPUT_DIR = r"C:\path\to\your\Data"

# BioD Contractor Data feature layer item ID (waypoints layer 0, polylines layer 1)
CONTRACTOR_ITEM_ID = "your-contractor-item-id"
WAYPOINTS_LAYER_ID = 0
POLYLINES_LAYER_ID = 1

# Animal Pest Control FeatureServer URL (trap features layer 0, inspection table layer 1)
TRAP_SERVICE_URL = "https://services1.arcgis.com/your-org-id/arcgis/rest/services/your-trap-layer/FeatureServer"
TRAP_LAYER_ID    = 0
INSP_TABLE_ID    = 1

# Priority Habitats spatial layer URL (used by Pressure_Management_Data_Join.py)
FEATURE_SERVICE_URL = "https://services1.arcgis.com/your-org-id/arcgis/rest/services/your-ph-layer/FeatureServer/0"

# PCO Management Dataset — RTCI monitoring results (read-only)
# Used by Manawatū Estuary dashboard (layer 0)
PCO_MONITORING_URL = "https://services1.arcgis.com/VuN78wcRdq1Oj69W/arcgis/rest/services/PCO_Management_Dataset/FeatureServer"

# eBird API key — used to fetch recent bird sightings near icon sites
# Keys do not expire. Generate or view yours at https://ebird.org/api/keygen
EBIRD_API_KEY = "your-ebird-api-key"

# ── Kia Whārite ───────────────────────────────────────────────────────────────

# Local File Geodatabase containing the PCO treatment area data.
# Layer: PCO_Treatment_Area_ExportFeatures  Fields used: PCOName, RTC
KIA_WHARITE_GDB = r"\\gisdata\GIS\Department\Environmental_Management\Biodiversity\BioData\Biodiversity\Icon Sites\Kia Wharite\Kia_Wharite_Project\Kia_Wharite_Project.gdb"
