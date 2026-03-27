# Data Input Requirements & Known Issues

This document tracks data quality requirements for the Pressure Management Reporting System Data spreadsheet (`Pressure Management Reporting System Data 2025.xlsx`) hosted on SharePoint at:

> Biodiversity > Programmes > Priority Habitats > Management levels > Management Measure system 2023

These requirements must be met for the Python automation script to process the data correctly.

---

## Raw site data tab

### Issues identified

**1. Missing column header — Region (Column A)**
The first column contains the region code (e.g. Horo, Man, Rang, Rua, Tara) but has no column header. The script expects this to be the first column and assigns it the name `Region` internally. This works currently but should be formally labelled for clarity.

**2. Blank/missing values in Region column**
The Region value is only entered once for the first row of each region group — subsequent rows for the same region are left blank. For the script to correctly assign each site to a region, every row must have its Region value filled in. Blank cells are read as `NaN` and will cause those sites to be excluded from regional summaries.
- **Action required:** Fill down the Region value for every row (no blank cells in Column A).

**3. Blank/missing values in other columns**
Some rows appear to have blank cells in columns such as Site Name, Ecosystem type, and Lead. Where these are genuinely unknown they should be marked explicitly (e.g. `Unknown` or `TBC`) rather than left blank, so the script can handle them consistently.

---

## General notes

- The script reads the `Raw site data` tab starting at row 4 (header row). Rows 1–3 are skipped. Do not add or remove rows above the header.
- Do not add, remove, or rename columns without updating the script accordingly.
- Filters applied to the sheet do not affect the script — all rows are read regardless of filter state.

---

*Last updated: 2026-03-25*
