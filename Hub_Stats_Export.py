"""
Hub_Stats_Export.py
===================

Builds html/hub_stats.json — the handful of headline figures shown in the
"Programme at a Glance" strip on html/index.html, plus the reporting period in
the header badge. index.html fetches that JSON at runtime, so the page itself
never has to be edited when a figure changes.

Same pattern as PM_Dashboard_Export.py and KKT_Dashboard_Export.py: the HTML is
static, only the JSON moves.

Where each figure comes from:

  Priority Habitat sites  Priority_Habitats_Pressure_Management layer 0 (the
                          layer Pressure_Management_Data_Join.py publishes).
                          One row per site, counted where site_programme is
                          'Priority Habitat' — so the five Icon Sites and Tōtara
                          Reserve carried in the same layer are excluded.

  KKT grants              Biodiversity_KKT_Projects layer 4. One row per grant,
                          so this is grants awarded across all years, not the
                          number of distinct projects (many groups are funded
                          more than once).

  Reporting period        The financial year just finished, worked out from
                          today's date. NZ financial years run 1 July to 30
                          June, so anything from 1 July 2026 onwards reports on
                          2025–26.

Run from the ArcGIS Pro Python environment, signed in to the portal in Pro:

    python Hub_Stats_Export.py            # write the JSON only
    python Hub_Stats_Export.py --push     # write, commit and push
"""

import os
import json
import logging
import argparse
import subprocess
from datetime import datetime as dt

from config import PH_PRESSURE_ITEM_ID, KKT_SERVICE_URL

PH_LAYER_ID = 0
KKT_LAYER_ID = 4

# The site_programme value that counts as a Priority Habitat site. Matches the
# classification Pressure_Management_Data_Join.py writes (everything not listed
# in its SPECIAL_SITES dict falls back to this).
PRIORITY_HABITAT_PROGRAMME = "Priority Habitat"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs", "hub")
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, dt.now().strftime('%Y-%m-%d_%H-%M-%S') + '_hub_stats.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.FileHandler(log_file, encoding='utf-8'),
              logging.StreamHandler()],
)
log = logging.getLogger(__name__)


def previous_financial_year(today=None):
    """The financial year that has most recently finished, as ('2025-26',
    '2025–26 Financial Year'). NZ FYs run 1 July - 30 June, so in August
    2026 the year just finished is 2025-26."""
    today = today or dt.now()
    # July onwards we are in the FY starting this calendar year, so the one just
    # finished started last year. Jan-June we are still in the FY that started
    # last year, so the finished one started the year before that.
    start = today.year - 1 if today.month >= 7 else today.year - 2
    short = f"{start}-{str(start + 1)[2:]}"
    label = f"{start}–{str(start + 1)[2:]} Financial Year"
    return short, label


def count_priority_habitat_sites(gis):
    """Distinct sites in the published pressure management layer whose
    site_programme is 'Priority Habitat'."""
    item = gis.content.get(PH_PRESSURE_ITEM_ID)
    if item is None:
        raise RuntimeError(
            f"Could not open AGOL item {PH_PRESSURE_ITEM_ID}. Check "
            f"PH_PRESSURE_ITEM_ID in config.py and that you are signed in."
        )
    layer = item.layers[PH_LAYER_ID]
    log.info(f"Reading {item.title} / {layer.properties.name}...")

    where = f"site_programme = '{PRIORITY_HABITAT_PROGRAMME}'"
    feats = layer.query(where=where, out_fields="site_id,site_programme",
                        return_geometry=False).features

    # Counted distinct rather than trusting the row count: the layer is meant to
    # be one row per site, but a duplicate site_id would silently inflate the
    # headline figure otherwise.
    site_ids = {f.attributes.get("site_id") for f in feats
                if f.attributes.get("site_id")}
    if len(site_ids) != len(feats):
        log.warning(f"  {len(feats)} rows but only {len(site_ids)} distinct "
                    f"site_id values — the layer has duplicates.")

    # Everything else in the layer is an Icon Site or the Regional Park. Logged
    # so a new programme value showing up is visible rather than silently
    # dropping out of the count.
    other = layer.query(where=f"site_programme <> '{PRIORITY_HABITAT_PROGRAMME}'",
                        out_fields="site_programme",
                        return_geometry=False).features
    other_counts = {}
    for f in other:
        key = f.attributes.get("site_programme") or "(blank)"
        other_counts[key] = other_counts.get(key, 0) + 1
    log.info(f"  {len(site_ids)} Priority Habitat sites "
             f"(also in layer: {other_counts or 'nothing'})")
    return len(site_ids)


def count_kkt_grants(gis):
    """Grant rows in the KKT projects layer — one row per grant awarded, across
    every funding round."""
    from arcgis.features import FeatureLayerCollection

    flc = FeatureLayerCollection(KKT_SERVICE_URL, gis=gis)
    layer = next(l for l in flc.layers if l.properties.id == KKT_LAYER_ID)
    log.info(f"Reading {layer.properties.name}...")

    feats = layer.query(where="1=1", out_fields="Project_ID,Grant_year_1",
                        return_geometry=False).features
    grants = len(feats)
    projects = len({f.attributes.get("Project_ID") for f in feats
                    if f.attributes.get("Project_ID")})
    years = sorted({f.attributes.get("Grant_year_1") for f in feats
                    if f.attributes.get("Grant_year_1")})
    log.info(f"  {grants} grants across {projects} distinct projects, "
             f"years {years}")
    return grants


def main():
    ap = argparse.ArgumentParser(description="Build the hub headline stats JSON.")
    ap.add_argument("--push", action="store_true",
                    help="Commit and push the JSON to GitHub Pages.")
    args = ap.parse_args()

    log.info("=" * 70)
    log.info("HUB STATS EXPORT")
    log.info("=" * 70)

    from arcgis.gis import GIS

    log.info("Connecting to AGOL using the ArcGIS Pro sign-in...")
    gis = GIS("home")

    ph_sites = count_priority_habitat_sites(gis)
    kkt_grants = count_kkt_grants(gis)
    fy_short, fy_label = previous_financial_year()
    log.info(f"Reporting period: {fy_label}")

    generated_at = dt.now().strftime('%d %B %Y %H:%M')
    payload = {
        "generated_at": generated_at,
        "priority_habitat_sites": ph_sites,
        "kkt_grants": kkt_grants,
        "reporting_period": fy_label,
        "reporting_period_short": fy_short,
    }

    out_path = os.path.join(SCRIPT_DIR, "html", "hub_stats.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    log.info(f"Written: {out_path}")

    if args.push:
        log.info("Committing and pushing to GitHub...")
        rel = "html/hub_stats.json"
        subprocess.run(['git', '-C', SCRIPT_DIR, 'add', rel], check=True)
        result = subprocess.run(
            ['git', '-C', SCRIPT_DIR, 'commit', '-m',
             f'Update hub headline stats ({generated_at})'],
            capture_output=True, text=True)
        if result.returncode == 0:
            subprocess.run(['git', '-C', SCRIPT_DIR, 'push', 'origin', 'main'],
                           check=True)
            log.info("Pushed to GitHub successfully.")
        else:
            log.info("No changes to commit — hub stats already up to date.")
    else:
        log.info("Not pushed (no --push). index.html reads the copy on GitHub "
                 "Pages, so push when you want the live hub to update.")

    log.info("=" * 70)
    log.info(f"Priority Habitat sites : {ph_sites}")
    log.info(f"KKT grants             : {kkt_grants}")
    log.info(f"Reporting period       : {fy_label}")
    log.info(f"Log                    : {log_file}")
    log.info("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
