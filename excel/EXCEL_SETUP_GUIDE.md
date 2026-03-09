# Excel Reporting Setup — Power Query, PivotTables & Solver

This document describes how to set up the Excel reporting layer for the FP&A Reporting Automation project.

---

## 1. Power Query: Connecting to PostgreSQL

### Option A: Direct ODBC Connection (Production)

1. **Install the PostgreSQL ODBC driver:**
   - Download from https://www.postgresql.org/ftp/odbc/versions/
   - Or install via Homebrew: `brew install psqlodbc`

2. **Create an ODBC Data Source:**
   - Open ODBC Data Source Administrator
   - Add a new User DSN → PostgreSQL Unicode
   - Server: `localhost`, Port: `5432`, Database: `fpa_reporting`

3. **In Excel → Data → Get Data → From ODBC:**
   - Select your DSN
   - Navigate to `gm_fpa` schema
   - Import these three views:
     - `vw_monthly_variance` → loads as query `qry_MonthlyVariance`
     - `vw_desk_summary` → loads as query `qry_DeskSummary`
     - `vw_daily_pnl_detail` → loads as query `qry_DailyPnL`

4. **Set auto-refresh:**
   - Right-click each query in the Queries pane → Properties
   - ☑ "Refresh data when opening the file"
   - ☑ "Refresh every 5 minutes" (for close week)

### Option B: CSV Import (Demo / Portfolio)

If you don't have PostgreSQL running, use the exported CSVs:

1. Run `python python/simulate_data.py` to generate raw CSVs in `data/raw/`
   (or run `python python/export_for_excel.py` after loading PostgreSQL for view-based exports in `data/processed/`)
2. In Excel → Data → Get Data → From File → From CSV
3. Import:
   - `monthly_variance.csv` → `qry_MonthlyVariance`
   - `desk_summary.csv` → `qry_DeskSummary`
   - `daily_pnl_detail.csv` → `qry_DailyPnL`

---

## 2. PivotTable Layouts

### Sheet 1: "Desk P&L Waterfall"

| Field            | Area     |
|------------------|----------|
| `desk_name`      | Rows     |
| `actual_pnl`     | Values (Sum) |
| `plan_pnl`       | Values (Sum) |
| `prior_pnl`      | Values (Sum) |
| `var_actual_vs_plan`  | Values (Sum) |
| `var_actual_vs_prior` | Values (Sum) |
| `year_month`     | Slicer   |
| `region`         | Slicer   |

**Formatting:**
- Conditional formatting on variance columns: green ≥ 0, red < 0
- Number format: `$#,##0` (thousands) or `$#,##0.0,,"M"` (millions)
- Insert a clustered bar chart beside the PivotTable (Actual vs Plan)

### Sheet 2: "Product Detail"

| Field            | Area     |
|------------------|----------|
| `desk_name`      | Rows (Level 1) |
| `product_name`   | Rows (Level 2, nested) |
| `actual_pnl`     | Values (Sum) |
| `plan_pnl`       | Values (Sum) |
| `var_actual_vs_plan` | Values (Sum) |
| `var_pct_vs_plan`    | Values (Avg) |
| `rorwa_annualized`   | Values (Avg) |
| `asset_class`    | Slicer   |
| `year_month`     | Slicer   |

**Usage:** Desk heads expand their row to see which products are driving their variance.

### Sheet 3: "Trend Analysis"

| Field            | Area     |
|------------------|----------|
| `year_month`     | Rows     |
| `desk_name`      | Columns  |
| `actual_pnl`     | Values (Sum) |

**Chart:** Line chart with:
- Series 1: Actual P&L (solid line)
- Series 2: Plan P&L (dashed line) — requires adding `plan_pnl` as a second value
- Series 3: Prior P&L (dotted line) — requires adding `prior_pnl` as a third value
- One line per desk, trailing 12 months or full date range (Apr 2018 – Jun 2025)

### Sheet 4: "Executive Summary" (Print-Ready)

This is a **formatted static sheet** (not a PivotTable) that uses `GETPIVOTDATA()` formulas:

```
Layout:
┌──────────────────────────────────────────────────────┐
│  GLOBAL MARKETS MONTHLY PERFORMANCE REPORT           │
│  Reporting Period: [year_month from dropdown]         │
│  Prepared by: FP&A Team | Date: [=TODAY()]           │
├──────────────────────────────────────────────────────┤
│                                                      │
│  FIRM TOTAL          Actual    Plan    Var    Prior   │
│  ─────────────────   ──────   ──────  ─────  ──────  │
│  Net P&L ($M)         XX.X     XX.X   +X.X    XX.X  │
│  RWA ($B)              X.X      X.X   +X.X     X.X  │
│  RoRWA (ann.)        XX.X%    XX.X%          XX.X%  │
│                                                      │
│  DESK BREAKDOWN      Actual    Plan    Var %  Status │
│  ─────────────────   ──────   ──────  ─────  ──────  │
│  Equities Cash        X.XM     X.XM   +X%     ●     │
│  Equity Derivatives   X.XM     X.XM   -X%     ●     │
│  FI Rates             X.XM     X.XM   +X%     ●     │
│  FX & Commodities     X.XM     X.XM   +X%     ●     │
│                                                      │
│  TOP 3 PRODUCTS      Actual   Var vs Plan            │
│  ─────────────────   ──────   ───────────            │
│  1. IRS               X.XM     +X.XM                │
│  2. Govt Bonds        X.XM     +X.XM                │
│  3. Spot FX           X.XM     +X.XM                │
│                                                      │
│  BOTTOM 3 PRODUCTS   Actual   Var vs Plan            │
│  ─────────────────   ──────   ───────────            │
│  1. Struct Notes      X.XM     -X.XM                │
│  2. FX Options        X.XM     -X.XM                │
│  3. Commodities       X.XM     -X.XM                │
│                                                      │
│  COMMENTARY:                                         │
│  [Analyst writes 2-3 lines of narrative here]        │
│                                                      │
└──────────────────────────────────────────────────────┘
```

**Formula examples:**
```excel
=GETPIVOTDATA("actual_pnl", 'Desk P&L Waterfall'!$A$3)          ' Firm total actual
=GETPIVOTDATA("actual_pnl", 'Desk P&L Waterfall'!$A$3, "desk_name", "Equities Cash Trading")
=GETPIVOTDATA("var_actual_vs_plan", 'Product Detail'!$A$3, "product_name", "Interest Rate Swaps")
```

---

## 3. Excel Solver: Capital Allocation Optimizer

### Sheet: "Capital Optimizer"

#### Layout

| Row | A                  | B             | C              | D              | E           |
|-----|--------------------|---------------|----------------|----------------|-------------|
| 1   | **Desk**           | **RWA Alloc ($M)** | **RoRWA (3M avg)** | **Expected P&L ($M)** | **Current RWA** |
| 2   | Equities Cash      | [DECISION]    | =from PivotTable | =B2*C2       | =from PT    |
| 3   | Equity Derivatives | [DECISION]    | =from PivotTable | =B3*C3       | =from PT    |
| 4   | FI Rates           | [DECISION]    | =from PivotTable | =B4*C4       | =from PT    |
| 5   | FX & Commodities   | [DECISION]    | =from PivotTable | =B5*C5       | =from PT    |
| 6   |                    |               |                |                |             |
| 7   | **Totals**         | =SUM(B2:B5)   |                | =SUM(D2:D5)   |             |
| 8   | **Objective →**    |               |                | **=D7** (MAX)  |             |
| 9   |                    |               |                |                |             |
| 10  | **CONSTRAINTS**    |               |                |                |             |
| 11  | Total RWA Cap      | 5,000         | $M             | B7 ≤ 5000      |             |
| 12  | Min per desk       | 500           | $M             | Each B ≥ 500   |             |
| 13  | Max per desk       | 2,000         | $M             | Each B ≤ 2000  |             |
| 14  | Min Exp. P&L       | [=gap/months] | $M             | D7 ≥ this      |             |

#### Solver Configuration

1. **Open:** Data → Solver
2. **Set Objective:** `$D$8`
3. **To:** Max
4. **By Changing Variable Cells:** `$B$2:$B$5`
5. **Subject to Constraints:**
   - `$B$7 <= $B$11`  (total RWA ≤ cap)
   - `$B$2:$B$5 >= $B$12` (each desk ≥ minimum)
   - `$B$2:$B$5 <= $B$13` (each desk ≤ maximum)
   - `$D$7 >= $B$14` (expected P&L ≥ gap target)
6. **Solving Method:** Simplex LP
7. **Click Solve**

#### Interpretation

The output tells the Head of Markets: "To maximize expected P&L next month while
staying within the $5B RWA cap, allocate $X.XB to Rates (highest RoRWA), $X.XB to
Equities Cash, etc." This replaces subjective capital allocation with a data-driven
recommendation and is presented alongside the monthly exec pack.

---

## 4. Refresh Workflow (Month-End Close)

### Before (Manual Process) — ~16 hours

| Step | Time   | Task |
|------|--------|------|
| 1    | 4 hrs  | Export trade blotters, copy into Excel |
| 2    | 6 hrs  | Build variance tables with VLOOKUP |
| 3    | 2 hrs  | Format executive summary |
| 4    | 4 hrs  | Field ad-hoc questions |

### After (Automated Pipeline) — ~2 hours

| Step | Time   | Task |
|------|--------|------|
| 1    | 5 min  | Run `python run_pipeline.py` |
| 2    | 1 min  | Open Excel → Refresh All |
| 3    | 30 min | Review numbers, write commentary |
| 4    | 5 min  | Export exec summary to PDF |
| 5    | 60 min | Review/sign-off buffer |

**Time saved: ~14 hours per month-end close (~75% reduction)**
