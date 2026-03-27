# config.example.py — copy this to config.py and fill in your local paths

# Local path to the manually exported SharePoint CSV (Raw site data tab)
CSV_PATH = r"C:\path\to\your\Data\Pressure Management Reporting System Data 2025(Raw site data).csv"

# Output file geodatabase
OUTPUT_GDB = r"\\your-server\your-share\PH_Pressure_Management\PH_Pressure_Management.gdb"

# Network directory for CSV outputs (pressure by ecosystem, threshold status)
NETWORK_DIR = r"\\your-server\your-share\PH_Pressure_Management"

# Local path for district summary CSV
OUTPUT_SUMMARY_CSV = r"C:\path\to\your\Outputs\PH_Pressure_Summary_by_District.csv"
