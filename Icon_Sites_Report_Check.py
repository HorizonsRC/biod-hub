"""
Icon_Sites_Report_Check.py
==========================
Dry-run report checker for the Icon Sites dashboards.

Reads the two written sources that the icon site dashboards are built from,
extracts the figures, compares them against what is currently published in the
HTML, and writes a review file listing every difference.

**This script never edits the dashboards and never pushes.** It only reports.
Applying a change is a manual decision, made after reading the review file.
That is deliberate: report layouts are outside our control and change without
warning, and `Icon_Sites_Data_Export.py` commits and pushes by itself — a
silent mis-parse feeding that path would publish a wrong figure to a public
dashboard with nothing to catch it.

Two sources
-----------
1. **ICC agendas** (public, automatic).  Horizons publishes Integrated
   Catchment Committee agendas as PDFs.  Since the November 2025 meeting each
   agenda carries one table per icon site, captioned
   ``Table N: Icon Site <name> - Background and activity for the reporting
   period``.  Those tables give a per-site activity narrative and the site's
   annual HRC budget.  Reports before November 2025 use a different layout
   (``Table 4: Biodiversity partnerships activity on the icon sites ...``) and
   are not parsed — the script says so rather than returning nothing.

2. **Grantee annual reports** (manual drop, for now).  PDFs placed in
   ``Data/Icon site data/``.  Each carries a funding-agreement table with fixed
   ``Category | Activities in Reporting Year | Statistics`` rows.  Those row
   labels are the anchor.  Only Bushy Park has one at present.  When SharePoint
   API access is available this folder read is the part that gets replaced —
   everything downstream stays the same.

Where the two sources disagree, both values are reported. They are written by
different people for different purposes and are not expected to agree
perfectly; the point is to see the disagreement rather than silently pick one.

Usage (ArcGIS Pro Python environment)::

    python Icon_Sites_Report_Check.py              # fetch latest, full check
    python Icon_Sites_Report_Check.py --no-fetch   # use cached PDFs only
    python Icon_Sites_Report_Check.py --keep 3     # check the last 3 agendas

Output: ``Data/icon-sites-review/review_<timestamp>.md`` plus a console summary.
Downloaded agendas are cached in ``Data/icc-reports/``.

If the meeting index comes back empty, the CMS node id in ``ICC_KIND`` has
probably changed. Re-read it from the live page: open the past-meetings page,
F12 -> Network -> Fetch/XHR, click a committee filter, and copy the ``kind``
query parameter from the ``pastmeetings`` request.
"""

import argparse
import datetime
import json
import logging
import re
import sys
import unicodedata
from pathlib import Path

import requests

try:
    import fitz  # PyMuPDF
except ImportError:
    sys.exit(
        "ERROR: PyMuPDF not available. Run from the ArcGIS Pro python "
        "environment, where it is installed as 'fitz'."
    )

# ── Logging ───────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent
LOG_DIR = ROOT / "logs" / "icon-sites"
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_path = LOG_DIR / f"report_check_{datetime.datetime.now():%Y%m%d_%H%M%S}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
# Public council web addresses, not credentials, so they live here rather than
# in config.py. ICC_KIND is the CMS node id of the past-meetings page; see the
# module docstring for how to re-read it if the page is rebuilt.
HORIZONS = "https://www.horizons.govt.nz"
ICC_ENDPOINT = f"{HORIZONS}/umbraco/surface/meetingsfilter/pastmeetings"
ICC_KIND = "7dd95dc4-7112-4d5c-8917-bc43867fe67a"
ICC_CATEGORY = "integrated-catchment-committee"
HTTP_TIMEOUT = 120

HTML_DIR = ROOT / "html" / "icon-sites"
LANDING_HTML = HTML_DIR / "icon-sites.html"
ANNUAL_REPORT_DIR = ROOT / "Data" / "Icon site data"
ICC_CACHE_DIR = ROOT / "Data" / "icc-reports"
REVIEW_DIR = ROOT / "Data" / "icon-sites-review"

# The per-site table caption used by every ICC agenda since November 2025.
ICC_SITE_CAPTION = re.compile(r"Table\s+(\d+):\s*Icon Site\s+([^\n]+)")
# The pre-November-2025 layout, recognised only so we can say why we skipped it.
ICC_LEGACY_CAPTION = re.compile(
    r"Table\s+\d+:\s*Biodiversity [Pp]artnerships activity", re.I
)

# Canonical site keys -> how each source names them.
# `icc` matches the caption text; `card` must equal the landing page card title.
SITES = {
    "bushy-park": {
        "card": "Bushy Park Tarapuruhi",
        "icc": re.compile(r"Bushy Park", re.I),
    },
    "te-apiti": {
        "card": "Te Āpiti Manawatū",
        "icc": re.compile(r"Te\s+[ĀA]piti", re.I),
    },
    "kia-wharite": {
        "card": "Kia Whārite Biodiversity Project",
        "icc": re.compile(r"Kia\s+Wh[āa]rite", re.I),
    },
    "manawatu-estuary": {
        "card": "Manawatū Estuary",
        "icc": re.compile(r"Manawat[ūu]\s+Estuary", re.I),
    },
    "pukaha": {
        "card": "Pūkaha Mt Bruce",
        "icc": re.compile(r"P[ūu]kaha", re.I),
    },
    "ruahine-kiwi": {
        "card": "Southern Ruahine Kiwi",
        "icc": re.compile(r"Ruahine Kiwi", re.I),
    },
}

# Row labels expected in the grantee annual report's funding-agreement table.
# Anything missing, or any label present that is not listed here, is reported
# as template drift — the format is the grantee's, not ours, and it will move.
EXPECTED_TEMPLATE_ROWS = [
    "project activity type",
    "area of whole community biodiversity project",
    "area of site directly relevant to this hrc-funded project",
    "number of visitors to whole biodiversity site",
    "number of volunteers involved in hrc-funded project",
    "total volunteer hours contributed to hrc-funded project",
    "plants planted as funded by this grant",
    "area planted with this grant",
    "traps funded by this grant",
    "total kills in traps funded",
    "bait stations funded",
    "ungulates controlled with this grant",
    "stems controlled",
    "area controlled",
    "top 3 species controlled",
    # norm_label() strips bracketed text, so "New asset(s) purchased" arrives
    # here as "new asset purchased".
    "new asset purchased",
    "size of asset",
    "monitoring method used",
]


# ── Small helpers ─────────────────────────────────────────────────────────────
def norm(text):
    """Collapse whitespace and normalise unicode for comparison."""
    text = unicodedata.normalize("NFC", text or "")
    return re.sub(r"\s+", " ", text).strip()


def norm_label(text):
    """Normalise a template row label for matching.

    PDF text extraction hyphen-breaks across line wraps ("HRC- funded"), so
    those are repaired before comparison, and anything after a bracketed
    example ("(e.g. ...)") is dropped.
    """
    s = norm(text).lower()
    s = s.replace("- ", "-")
    s = re.sub(r"\(e\.g\.[^)]*\)?", "", s)
    s = re.sub(r"\(.*?\)", "", s)
    return re.sub(r"\s+", " ", s).strip(" :")


def first_number(text):
    """First integer in a string, commas tolerated. None if there isn't one."""
    m = re.search(r"\d[\d,]*", text or "")
    return int(m.group(0).replace(",", "")) if m else None


def fmt(value):
    return "—" if value is None else str(value)


# ── ICC agendas ───────────────────────────────────────────────────────────────
def fetch_meeting_index(category=ICC_CATEGORY):
    """Return past ICC meetings, newest first.

    Each entry is ``{date, slug, title, pdf_url}``. The meeting date is taken
    from the card id rather than the PDF filename — several agendas are simply
    named ``public-agenda.pdf`` with no date in them.
    """
    params = {
        "category": category,
        "orderby": "latest",
        "page": 1,
        "kind": ICC_KIND,
    }
    log.info("Fetching ICC meeting index …")
    resp = requests.get(ICC_ENDPOINT, params=params, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    html = resp.text

    meetings = []
    # Cards carry an id like "integrated-catchment-committee-29-july-2026-2235";
    # the agenda link is the first PDF href after it.
    card_re = re.compile(r'id="(' + re.escape(category) + r'-(\d{1,2})-([a-z]+)-(\d{4})[^"]*)"')
    months = {m.lower(): i for i, m in enumerate(
        ["january", "february", "march", "april", "may", "june", "july",
         "august", "september", "october", "november", "december"], start=1)}

    positions = [(m.start(), m) for m in card_re.finditer(html)]
    for idx, (pos, m) in enumerate(positions):
        slug, day, month_name, year = m.groups()
        month = months.get(month_name.lower())
        if not month:
            log.warning("Unrecognised month in card id: %s", slug)
            continue
        # A card's agenda link sits after its id attribute, so the span to
        # search runs from this id up to the next card's id. Bounding it at the
        # next card matters: without that, a card whose agenda is missing
        # silently picks up the following meeting's PDF.
        end = positions[idx + 1][0] if idx + 1 < len(positions) else len(html)
        segment = html[pos:end]
        pdfs = re.findall(r'href="(/media/[^"]+\.pdf)"', segment)
        agenda = next((p for p in pdfs if "agenda" in p.lower()), None)
        if not agenda:
            log.warning("No agenda PDF found for %s", slug)
            continue
        meetings.append({
            "date": datetime.date(int(year), month, int(day)),
            "slug": slug,
            "pdf_url": HORIZONS + agenda,
        })

    meetings.sort(key=lambda r: r["date"], reverse=True)
    log.info("Found %d ICC meetings with agendas", len(meetings))
    if not meetings:
        log.error(
            "Meeting index came back empty. The ICC_KIND node id has probably "
            "changed — see the module docstring for how to re-read it."
        )
    return meetings


def sidecar_for(pdf_path):
    """Path of the small JSON note recording where a cached agenda came from."""
    return pdf_path.with_suffix(".json")


def write_sidecar(pdf_path, meeting):
    """Record a cached agenda's date and source URL next to the PDF.

    Without this a `--no-fetch` run knows the file but not the URL it came
    from, so it cannot rebuild the deep link to compare against.
    """
    sidecar_for(pdf_path).write_text(
        json.dumps({"date": meeting["date"].isoformat(),
                    "pdf_url": meeting["pdf_url"]}, indent=2),
        encoding="utf-8",
    )


def read_sidecar(pdf_path):
    """Rebuild a meeting record from a cached agenda's sidecar, if present."""
    path = sidecar_for(pdf_path)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {"date": datetime.date.fromisoformat(data["date"]),
                "pdf_url": data["pdf_url"]}
    except (ValueError, KeyError) as exc:
        log.warning("Could not read %s: %s", path.name, exc)
        return None


def download_pdf(url, dest):
    """Download `url` to `dest` unless it is already cached."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        log.info("Cached: %s", dest.name)
        return dest
    log.info("Downloading %s …", url)
    resp = requests.get(url, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    log.info("Saved %s (%.1f MB)", dest.name, len(resp.content) / 1e6)
    return dest


def extract_icc_sections(pdf_path):
    """Split an ICC agenda into per-site sections.

    Returns ``(sections, warnings)`` where sections maps a canonical site key to
    ``{caption, page, text}``. An agenda in the pre-November-2025 layout yields
    no sections and one warning saying so — never a silent empty result.
    """
    doc = fitz.open(pdf_path)
    pages = [p.get_text() for p in doc]
    full = "".join(pages)

    # Page number for a character offset, so findings can cite a page.
    offsets, running = [], 0
    for text in pages:
        offsets.append(running)
        running += len(text)

    def page_for(pos):
        page = 0
        for i, start in enumerate(offsets):
            if start <= pos:
                page = i
            else:
                break
        return page + 1

    captions = list(ICC_SITE_CAPTION.finditer(full))
    warnings = []

    if not captions:
        if ICC_LEGACY_CAPTION.search(full):
            warnings.append(
                f"{pdf_path.name}: uses the pre-Nov-2025 layout "
                "('Biodiversity partnerships activity on the icon sites'), "
                "which has no per-site tables. Not parsed."
            )
        else:
            warnings.append(
                f"{pdf_path.name}: no 'Table N: Icon Site ...' captions found. "
                "The agenda layout has changed — the anchor needs revisiting."
            )
        doc.close()
        return {}, warnings

    sections = {}
    for i, m in enumerate(captions):
        end = captions[i + 1].start() if i + 1 < len(captions) else len(full)
        caption = norm(m.group(2))
        body = full[m.start():end]
        key = next((k for k, cfg in SITES.items() if cfg["icc"].search(caption)), None)
        if not key:
            warnings.append(
                f"{pdf_path.name}: table caption '{caption}' did not match any "
                "known icon site."
            )
            continue
        sections[key] = {
            "caption": caption,
            "page": page_for(m.start()),
            "text": body,
        }

    missing = [k for k in SITES if k not in sections]
    if missing:
        warnings.append(
            f"{pdf_path.name}: no table found for {', '.join(sorted(missing))}."
        )

    doc.close()
    return sections, warnings


def budget_from_section(text):
    """Pull the HRC annual budget out of an ICC site section.

    Prefers an amount on the line that mentions an annual budget or annual
    funding; otherwise falls back to the first amount in the section. Formats
    are inconsistent between sites ($424k vs $110,000), so the raw string is
    returned alongside a normalised integer where one can be derived.
    """
    amounts = re.findall(r"\$[\d,]+(?:\.\d+)?\s*[kKmM]?", text)
    if not amounts:
        return None, None

    preferred = None
    for line in text.split("\n"):
        if re.search(r"annual (budget|funding)|budget of", line, re.I):
            found = re.findall(r"\$[\d,]+(?:\.\d+)?\s*[kKmM]?", line)
            if found:
                preferred = found[0]
                break
    raw = norm(preferred or amounts[0])

    m = re.match(r"\$([\d,]+(?:\.\d+)?)\s*([kKmM]?)", raw)
    value = None
    if m:
        value = float(m.group(1).replace(",", ""))
        suffix = m.group(2).lower()
        value *= {"k": 1_000, "m": 1_000_000}.get(suffix, 1)
        value = int(round(value))
    return raw, value


# ── Grantee annual reports ────────────────────────────────────────────────────
def parse_annual_report(pdf_path):
    """Extract the funding-agreement table from a grantee annual report.

    Returns ``(rows, meta, warnings)``. `rows` maps a normalised row label to
    ``{label, value, page}``. Rows whose value wraps onto following table rows
    (the trap breakdown does) are stitched back together.
    """
    doc = fitz.open(pdf_path)
    rows, warnings = {}, []
    last_key = None

    for page_no, page in enumerate(doc, start=1):
        for table in page.find_tables().tables:
            for raw_row in table.extract():
                cells = [norm(c) for c in raw_row if norm(c)]
                if not cells:
                    continue
                if cells[0].lower().startswith("category"):
                    continue  # header

                # A row with a single cell is a continuation of the previous
                # row's value — the table wraps long values across rows.
                if len(cells) == 1:
                    if last_key:
                        rows[last_key]["value"] = norm(
                            rows[last_key]["value"] + " " + cells[0]
                        )
                    continue

                if len(cells) < 3:
                    continue
                category, label, value = cells[0], cells[1], " ".join(cells[2:])
                key = norm_label(label)
                if not key:
                    continue
                rows[key] = {
                    "category": category,
                    "label": norm(label),
                    "value": norm(value),
                    "page": page_no,
                }
                last_key = key

    if not rows:
        warnings.append(
            f"{pdf_path.name}: no funding-agreement table found. The report "
            "template has changed, or the table is an image rather than text."
        )

    # Template drift: compare the row labels we found against the ones we know.
    found = set(rows)
    for expected in EXPECTED_TEMPLATE_ROWS:
        if not any(expected in k for k in found):
            warnings.append(
                f"{pdf_path.name}: expected template row '{expected}' is missing."
            )
    for key in sorted(found):
        if not any(exp in key for exp in EXPECTED_TEMPLATE_ROWS):
            warnings.append(
                f"{pdf_path.name}: unrecognised template row '{key}' — the "
                "reporting template may have gained a field."
            )

    meta = {"pages": doc.page_count, "prose": "".join(p.get_text() for p in doc)}
    doc.close()
    return rows, meta, warnings


def lookup(rows, needle):
    """Find a template row by partial label match."""
    for key, row in rows.items():
        if needle in key:
            return row
    return None


def bushy_park_figures(rows, prose):
    """Map the Bushy Park annual report onto the dashboard's fields.

    Returns ``(figures, warnings)``. Figures are keyed by the dashboard label
    they belong to, so the comparison can be a straight lookup.
    """
    figures, warnings = {}, []

    def take(field, needle, transform=first_number):
        row = lookup(rows, needle)
        if not row:
            warnings.append(f"Bushy Park: no template row matching '{needle}'.")
            return
        figures[field] = {
            "value": transform(row["value"]),
            "raw": row["value"],
            "page": row["page"],
        }

    take("Volunteer Hours", "total volunteer hours")
    take("Volunteers", "number of volunteers involved")
    take("Sanctuary Area Protected", "area of site directly relevant")
    take("Plants Planted", "plants planted as funded")
    take("Area Planted", "area planted with this grant")

    # Trap breakdown. The stated network totals and the itemised parts have
    # disagreed in every report seen so far, so both are reported.
    traps = lookup(rows, "traps funded by this grant")
    if traps:
        text = traps["value"]
        items = re.findall(r"(\d+)\s*x?\s*([A-Za-z][A-Za-z0-9 ]{1,20}?)(?=[,.]|\s+=|$)", text)
        stated = {
            "total": None,
            "halo": None,
            "forest": None,
        }
        m = re.search(r"Total Traps\s*=\s*(\d+)", text, re.I)
        if m:
            stated["total"] = int(m.group(1))
        for net in ("halo", "forest"):
            m = re.search(net + r" network:(.*?)=\s*(\d+)\s*Traps", text, re.I | re.S)
            if m:
                stated[net] = int(m.group(2))
        figures["Trap Network"] = {
            "value": stated["total"],
            "raw": text,
            "page": traps["page"],
            "detail": items,
        }
        if stated["halo"] and stated["forest"] and stated["total"]:
            if stated["halo"] + stated["forest"] != stated["total"]:
                warnings.append(
                    f"Bushy Park: stated trap total {stated['total']} does not "
                    f"equal halo {stated['halo']} + forest {stated['forest']} "
                    f"= {stated['halo'] + stated['forest']} (p.{traps['page']})."
                )

    # Pest plant stems, per species.
    stems = lookup(rows, "stems controlled")
    if stems:
        pairs = re.findall(r"([A-Za-z][A-Za-z'’ ]+?)\s*:\s*(\d+)", stems["value"])
        figures["Pest Plant Stems"] = {
            "value": {norm(name): int(count) for name, count in pairs},
            "raw": stems["value"],
            "page": stems["page"],
        }

    # Figures that live only in the report's prose, not the template table.
    # These are the fragile ones — flagged as such in the review file.
    prose_targets = {
        "Hihi Chicks Fledged": r"(\d+)\s+chicks fledged",
        "Halo Pests Removed": r"total of\s+([\d,]+)\s+pests removed",
        "Rats Eradicated": r"eradicat\w*\s+(\w+)\s+rats",
        "Plants In Nursery": r"[Aa]nother\s+([\d,]+)\s+plants have been sourced",
    }
    words = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
             "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10}
    for field, pattern in prose_targets.items():
        m = re.search(pattern, prose)
        if not m:
            warnings.append(f"Bushy Park: prose figure '{field}' not found.")
            continue
        token = m.group(1)
        value = words.get(token.lower(), first_number(token))
        figures[field] = {"value": value, "raw": norm(m.group(0)),
                          "page": None, "prose": True}

    return figures, warnings


# ── Reading what is currently published ───────────────────────────────────────
def read_dashboard_stats(html_path):
    """Read the 'Programme at a Glance' tiles from a site dashboard."""
    html = html_path.read_text(encoding="utf-8")
    pattern = re.compile(
        r'<div class="s-val[^"]*">([^<]+)</div>\s*<div class="s-key">([^<]+)</div>'
    )
    return {norm(key): norm(val) for val, key in pattern.findall(html)}


def read_chart_dataset(html_path, canvas_id, dataset_label):
    """Read one Chart.js dataset's data array from a dashboard."""
    html = html_path.read_text(encoding="utf-8")
    block = re.search(
        re.escape(canvas_id) + r"'\).*?\n\}\);", html, re.S
    )
    if not block:
        return None
    m = re.search(
        r"label:\s*'" + re.escape(dataset_label) + r"',\s*data:\s*\[([^\]]*)\]",
        block.group(0),
    )
    if not m:
        return None
    return [int(float(v)) for v in m.group(1).split(",") if v.strip()]


def read_landing_budgets(html_path):
    """Read each card's 'HRC Annual Budget' from the landing page."""
    html = html_path.read_text(encoding="utf-8")
    budgets = {}
    chunks = re.split(r'class="card-title">', html)[1:]
    for chunk in chunks:
        title = norm(chunk.split("<")[0])
        m = re.search(
            r'meta-key">HRC Annual Budget</div>\s*<div class="meta-val">([^<]+)</div>',
            chunk,
        )
        if m:
            budgets[title] = norm(m.group(1))
    return budgets


def budget_to_int(raw):
    m = re.match(r"\$([\d,]+(?:\.\d+)?)\s*([kKmM]?)", norm(raw or ""))
    if not m:
        return None
    value = float(m.group(1).replace(",", ""))
    value *= {"k": 1_000, "m": 1_000_000}.get(m.group(2).lower(), 1)
    return int(round(value))


# ── Comparison ────────────────────────────────────────────────────────────────
def compare_bushy_park(figures):
    """Diff the annual report's figures against bushy-park.html."""
    html_path = HTML_DIR / "bushy-park.html"
    stats = read_dashboard_stats(html_path)
    findings = []

    def add(field, published, reported, source):
        if reported is None:
            status = "NOT IN REPORT"
        elif published is None:
            status = "NOT ON PAGE"
        elif first_number(str(published)) == first_number(str(reported)):
            status = "match"
        else:
            status = "CHANGED"
        findings.append({
            "field": field,
            "published": published,
            "reported": reported,
            "status": status,
            "source": source,
        })

    tile_map = {
        "Volunteer Hours": "Volunteer Hours",
        "Volunteers": "Volunteers",
        "Sanctuary Area Protected": "Sanctuary Area Protected",
        "Hihi Chicks Fledged": "Hihi Chicks Fledged",
    }
    for field, tile in tile_map.items():
        fig = figures.get(field)
        page = fig.get("page") if fig else None
        source = f"annual report p.{page}" if page else "annual report (prose)"
        add(field, stats.get(tile), fig["value"] if fig else None, source)

    planted = read_chart_dataset(html_path, "revetChart", "Planted in ground")
    nursery = read_chart_dataset(html_path, "revetChart", "Raised in nursery")
    fig = figures.get("Plants Planted")
    add("Plants planted (chart)", planted[0] if planted else None,
        fig["value"] if fig else None,
        f"annual report p.{fig['page']}" if fig else "—")
    fig = figures.get("Plants In Nursery")
    add("Plants in nursery (chart)", nursery[0] if nursery else None,
        fig["value"] if fig else None, "annual report (prose)")

    # The halo catches chart is hand-entered — its source table is a pasted
    # image in the report, not text, so it cannot be extracted. The caption
    # above it is text, though, so the chart's total can still be checked.
    halo = figures.get("Halo Pests Removed")
    if halo and halo["value"]:
        charted = read_chart_dataset(html_path, "catchChart", "Caught")
        total = sum(charted) if charted else None
        findings.append({
            "field": "Halo catches — chart total vs report",
            "published": total,
            "reported": halo["value"],
            "status": "match" if total == halo["value"] else "CHANGED",
            "source": "hand-entered chart, total checked against the report",
        })

    stems = figures.get("Pest Plant Stems")
    if stems:
        published = read_chart_dataset(html_path, "weedChart", "2025–26")
        findings.append({
            "field": "Pest plant stems (chart)",
            "published": published,
            "reported": stems["value"],
            "status": "review",
            "source": f"annual report p.{stems['page']}",
        })

    return findings


def compare_report_link(meeting, icc_sections):
    """Check each dashboard's ICC report button against the latest agenda.

    The button deep-links into the agenda PDF at the site's own table
    (`...pdf#page=N`), so it goes stale the moment a newer agenda is published.
    Only Bushy Park carries one so far.
    """
    findings = []
    for key in ("bushy-park",):
        html_path = HTML_DIR / f"{key}.html"
        if not html_path.exists():
            continue
        html = html_path.read_text(encoding="utf-8")
        m = re.search(
            r'class="report-btn"[^>]*?href="([^"]+)"\s*>(.*?)</a>', html, re.S
        )
        published = m.group(1) if m else None
        label = norm(re.sub(r"&#\d+;", "", m.group(2))) if m else None
        section = icc_sections.get(key)
        expected = None
        if meeting and section:
            expected = f"{meeting['pdf_url']}#page={section['page']}"
        if expected is None:
            status = "NO AGENDA SECTION"
        elif published is None:
            status = "NO BUTTON ON PAGE"
        elif published == expected:
            status = "match"
        else:
            status = "CHANGED"
        # The button carries the report date in its text. A refreshed href with
        # a stale label reads as wrong to anyone looking at the dashboard, so
        # check the two agree.
        if meeting and label and status == "match":
            month_year = f"{meeting['date']:%b %Y}"
            if month_year.lower() not in label.lower():
                status = f"LABEL STALE (says '{label}', agenda is {month_year})"

        findings.append({
            "site": SITES[key]["card"],
            "published": published,
            "reported": expected,
            "status": status,
        })
    return findings


def compare_budgets(icc_sections):
    """Diff each ICC site budget against the landing page card."""
    published = read_landing_budgets(LANDING_HTML)
    findings = []
    for key, cfg in SITES.items():
        section = icc_sections.get(key)
        card = cfg["card"]
        page_value = published.get(card)
        if not section:
            findings.append({
                "site": card, "published": page_value, "reported": None,
                "status": "NOT IN AGENDA", "page": None,
            })
            continue
        raw, value = budget_from_section(section["text"])
        page_int = budget_to_int(page_value)
        if value is None:
            status = "NO AMOUNT FOUND"
        elif page_int is None:
            status = "NOT ON PAGE"
        elif page_int == value:
            status = "match"
        else:
            status = "CHANGED"
        findings.append({
            "site": card, "published": page_value, "reported": raw,
            "status": status, "page": section["page"],
        })
    return findings


# ── Review file ───────────────────────────────────────────────────────────────
def write_review(meeting, bp_findings, budget_findings, link_findings, warnings):
    REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now()
    out = REVIEW_DIR / f"review_{stamp:%Y%m%d_%H%M%S}.md"

    lines = [
        "# Icon Sites report check",
        "",
        f"Generated {stamp:%d %B %Y, %H:%M}. **Nothing has been changed** — this",
        "is a review file. Apply anything you agree with by hand.",
        "",
    ]

    if meeting:
        lines += [
            f"Latest ICC agenda: **{meeting['date']:%d %B %Y}** — {meeting['pdf_url']}",
            "",
        ]

    if warnings:
        lines += ["## Warnings", ""]
        lines += [f"- {w}" for w in warnings] + [""]
    else:
        lines += ["## Warnings", "", "None — both sources parsed as expected.", ""]

    lines += [
        "## Bushy Park — annual report vs dashboard",
        "",
        "| Field | On the page | In the report | Status | Source |",
        "|---|---|---|---|---|",
    ]
    for f in bp_findings:
        lines.append(
            f"| {f['field']} | {fmt(f['published'])} | {fmt(f['reported'])} "
            f"| {f['status']} | {f['source']} |"
        )
    lines.append("")

    if link_findings:
        lines += [
            "## ICC report deep links — dashboard button vs latest agenda",
            "",
            "| Site | On the page | Should be | Status |",
            "|---|---|---|---|",
        ]
        for f in link_findings:
            lines.append(
                f"| {f['site']} | {fmt(f['published'])} | {fmt(f['reported'])} "
                f"| {f['status']} |"
            )
        lines.append("")

    lines += [
        "## HRC annual budgets — ICC agenda vs landing page",
        "",
        "| Site | On the page | In the agenda | Status | Page |",
        "|---|---|---|---|---|",
    ]
    for f in budget_findings:
        lines.append(
            f"| {f['site']} | {fmt(f['published'])} | {fmt(f['reported'])} "
            f"| {f['status']} | {fmt(f['page'])} |"
        )
    lines.append("")

    out.write_text("\n".join(lines), encoding="utf-8")
    log.info("Review written to %s", out)
    return out


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-fetch", action="store_true",
                        help="use cached agendas only, do not hit the website")
    parser.add_argument("--keep", type=int, default=1,
                        help="how many recent agendas to download (default 1)")
    parser.add_argument("--report", metavar="NAME",
                        help="check a specific annual report by filename "
                             "fragment instead of the most recent")
    args = parser.parse_args()

    warnings = []
    meeting, icc_sections = None, {}

    # ── ICC agendas ──
    if args.no_fetch:
        cached = sorted(ICC_CACHE_DIR.glob("*.pdf"), reverse=True)
        if not cached:
            log.warning("No cached agendas in %s", ICC_CACHE_DIR)
        else:
            icc_sections, w = extract_icc_sections(cached[0])
            warnings += w
            meeting = read_sidecar(cached[0])
            if meeting is None:
                warnings.append(
                    f"{cached[0].name}: no sidecar recording its source URL, so "
                    "the report deep link cannot be checked. Run without "
                    "--no-fetch once to record it."
                )
    else:
        try:
            meetings = fetch_meeting_index()
        except requests.RequestException as exc:
            warnings.append(f"Could not reach the meetings page: {exc}")
            meetings = []
        for m in meetings[: args.keep]:
            dest = ICC_CACHE_DIR / f"icc_{m['date']:%Y%m%d}.pdf"
            try:
                download_pdf(m["pdf_url"], dest)
            except requests.RequestException as exc:
                warnings.append(f"Could not download {m['pdf_url']}: {exc}")
                continue
            write_sidecar(dest, m)
            sections, w = extract_icc_sections(dest)
            warnings += w
            if sections and not icc_sections:
                meeting, icc_sections = m, sections

    # ── Grantee annual reports ──
    bp_findings = []
    reports = sorted(ANNUAL_REPORT_DIR.glob("*.pdf"), reverse=True)
    bushy = [p for p in reports if "bushy park" in p.name.lower()]
    if args.report:
        bushy = [p for p in bushy if args.report.lower() in p.name.lower()]
    if not bushy:
        warnings.append(
            f"No Bushy Park annual report found in {ANNUAL_REPORT_DIR}. "
            "Export the latest from SharePoint and drop it in."
        )
    else:
        latest = bushy[0]
        log.info("Reading annual report: %s", latest.name)
        rows, meta, w = parse_annual_report(latest)
        warnings += w
        figures, w = bushy_park_figures(rows, meta["prose"])
        warnings += w
        bp_findings = compare_bushy_park(figures)

    budget_findings = compare_budgets(icc_sections) if icc_sections else []
    link_findings = compare_report_link(meeting, icc_sections) if icc_sections else []

    out = write_review(meeting, bp_findings, budget_findings, link_findings, warnings)

    changed = [f for f in bp_findings + budget_findings + link_findings
               if f["status"] not in ("match", "review")]
    log.info("-" * 60)
    log.info("%d figures need a look, %d warnings", len(changed), len(warnings))
    log.info("Review file: %s", out)
    log.info("Nothing was edited or pushed.")


if __name__ == "__main__":
    main()
