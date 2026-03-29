import os
import json
import logging
import subprocess
from datetime import datetime as dt
import pandas as pd

from config import NETWORK_DIR

# ============================================================
# LOGGING
# ============================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs", "pressure-management")
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, dt.now().strftime('%Y-%m-%d_%H-%M-%S') + '_export.log')

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

log.info("Starting PM Dashboard Export")

# ============================================================
# READ CSVs
# ============================================================

SCORES_CSV    = os.path.join(NETWORK_DIR, "PH_Pressure_Scores_Overview.csv")
ECOSYSTEM_CSV = os.path.join(NETWORK_DIR, "PH_Pressure_by_Ecosystem.csv")
THRESHOLD_CSV = os.path.join(NETWORK_DIR, "PH_Sites_Threshold_Status.csv")

log.info(f"Reading: {SCORES_CSV}")
scores_df = pd.read_csv(SCORES_CSV)

log.info(f"Reading: {ECOSYSTEM_CSV}")
ecosystem_df = pd.read_csv(ECOSYSTEM_CSV)

log.info(f"Reading: {THRESHOLD_CSV}")
threshold_df = pd.read_csv(THRESHOLD_CSV)

log.info(f"  Scores rows    : {len(scores_df)}")
log.info(f"  Ecosystem rows : {len(ecosystem_df)}")
log.info(f"  Threshold rows : {len(threshold_df)}")

# ============================================================
# BUILD dashboard_data.json
# ============================================================

generated_at = dt.now().strftime('%d %B %Y %H:%M')

payload = {
    "generated_at":        generated_at,
    "scores_overview":     scores_df.to_dict(orient='records'),
    "pressure_by_ecosystem": ecosystem_df.to_dict(orient='records'),
    "threshold_status":    threshold_df.to_dict(orient='records'),
}

JSON_OUT = os.path.join(SCRIPT_DIR, "html", "pressure-management", "dashboard_data.json")
with open(JSON_OUT, 'w', encoding='utf-8') as f:
    json.dump(payload, f)

log.info(f"Written: {JSON_OUT}")

# ============================================================
# GIT COMMIT & PUSH
# ============================================================

log.info("Committing and pushing dashboard_data.json to GitHub...")

try:
    subprocess.run(
        ['git', '-C', SCRIPT_DIR, 'add', 'html/pressure-management/dashboard_data.json'],
        check=True
    )
    result = subprocess.run(
        ['git', '-C', SCRIPT_DIR, 'commit', '-m',
         f'Update pressure management dashboard data ({generated_at})'],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        log.info(f"Committed: {result.stdout.strip()}")
        subprocess.run(
            ['git', '-C', SCRIPT_DIR, 'push', 'origin', 'main'],
            check=True
        )
        log.info("Pushed to GitHub successfully.")
    else:
        # Nothing to commit (data unchanged)
        log.info("No changes to commit — dashboard data is already up to date.")

except subprocess.CalledProcessError as e:
    log.error(f"Git operation failed: {e}")
    raise

# ============================================================
# COMPLETE
# ============================================================

log.info("=" * 70)
log.info("EXPORT COMPLETE")
log.info("=" * 70)
log.info(f"  dashboard_data.json : {JSON_OUT}")
log.info(f"  Generated at        : {generated_at}")
log.info(f"  Log                 : {log_file}")
log.info("=" * 70)
