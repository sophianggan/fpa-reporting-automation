# FP&A Reporting Automation

> **Improved close reporting speed, as measured by automated refreshes and Actual vs Plan vs Prior variance outputs, by building a monthly SQL data model and executive-summary pipeline.**

*Excel (Power Query, PivotTables, Solver) · SQL (PostgreSQL) · Tableau · Python*

---

## 📋 Project Overview

This project automates the month-end close reporting process for a Global Markets Finance / FP&A team covering four trading desks: Equities Cash, Equity Derivatives, Fixed Income Rates, and FX & Commodities.

**Problem:** Month-end close took ~16 hours (2 analyst-days) of manual CSV exports, VLOOKUP-driven variance tables, and formatting — producing a stale, error-prone report with no self-serve capability for desk heads.

**Solution:** An end-to-end pipeline that:
1. **Python** ingests real market data, validates it, and loads it into a centralized database
2. **PostgreSQL** serves as the single source of truth with pre-built reporting views
3. **Excel Power Query** refreshes PivotTables and an executive summary in seconds
4. **Tableau** provides daily interactive dashboards for desk heads and management
5. **Excel Solver** optimizes next-month capital allocation across desks

**Result:** Month-end close reduced from ~16 hours to ~2 hours (~75% reduction), with daily visibility replacing monthly-only reporting.

---

## 📊 Data Sources

The pipeline is driven by **real market data**, not synthetic/random numbers:

| Dataset | Rows | Date Range | Description |
|---------|------|------------|-------------|
| `macro_data_25yrs.csv` | 1,890 | Apr 2018 – Jun 2025 | M2 Money Supply, 10Y Treasury Yield, Fed Funds Rate, CPI, Inflation %, SOFR |
| `financial_timeseries_dataset.csv` | 3,741 | Aligned period | Standardized returns for AAPL, GOOGL, MSFT, AMZN, JPM (t-0 through t-29 lags) |

### Stock → Desk Mapping

Real equity returns are mapped to trading desk P&L using a weighted transformation with macro overlays:

| Stock | Desk | Products | Rationale |
|-------|------|----------|-----------|
| **AAPL** | Equities Cash | US Cash Eq, ETF Market Making | Large-cap US flow proxy |
| **MSFT** | Equities Cash | CA Cash Eq, Intl Cash Eq | Global equity flow proxy |
| **GOOGL** | Equity Derivatives | Index Opts, Single-Stock Opts, Struct Notes | Tech vol / options proxy |
| **GOOGL+MSFT** | Equity Derivatives | Equity Swaps | Blended signal for TRS book |
| **JPM** | Fixed Income Rates | Govt Bonds, IRS, Repo, Credit | Bank / rates proxy |
| **AMZN** | FX & Commodities | Spot FX, FX Fwd, FX Opts, Commodities | High-vol global trade proxy |

**Macro overlays:** 10Y yield changes amplify FI Rates P&L · SOFR level boosts Repo carry · Fed Funds changes affect FX vol

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                       DATA SOURCES (data/source/)                    │
│  Real market data (Apr 2018 – Jun 2025):                             │
│  • macro_data_25yrs.csv           (M2, 10Y, Fed Funds, CPI, SOFR)   │
│  • financial_timeseries_dataset.csv (AAPL/GOOGL/MSFT/AMZN/JPM)      │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     PYTHON PIPELINE                                  │
│                                                                      │
│  simulate_data.py → validate.py → load_to_pg.py                     │
│                                                                      │
│  • Map real stock returns to desk P&L via weighted transforms        │
│  • Apply macro overlays (rates, SOFR carry, FX vol)                  │
│  • Data quality: nulls, referential integrity, outliers              │
│  • Transform & bulk-load into PostgreSQL                             │
│                   Orchestrated by: run_pipeline.py                    │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    POSTGRESQL (gm_fpa schema)                        │
│                                                                      │
│  Dimensions:  dim_desk · dim_product · dim_calendar · dim_fx_rate   │
│  Facts:       fact_daily_pnl · fact_plan · fact_prior_year          │
│  Views:       vw_monthly_variance · vw_desk_summary                 │
│               vw_ytd_summary · vw_daily_pnl_detail                  │
└──────────────┬──────────────────────────────┬───────────────────────┘
               │                              │
               ▼                              ▼
┌──────────────────────────┐    ┌──────────────────────────────────┐
│      EXCEL REPORTING      │    │       TABLEAU DASHBOARDS          │
│                            │    │                                    │
│ Power Query → PivotTables  │    │ 1. Daily Desk P&L Scorecard      │
│ • Desk P&L Waterfall       │    │ 2. Actual vs Plan vs Prior Trend │
│ • Product Detail Drill     │    │ 3. RoRWA & Capital Efficiency    │
│ • 12-Month Trend           │    │                                    │
│ • Executive Summary (PDF)  │    │ Refreshes daily at 06:30          │
│ • Solver Capital Optimizer │    │                                    │
└──────────────────────────┘    └──────────────────────────────────┘
```

---

## 📁 Project Structure

```
fp&a/
├── README.md                    ← You are here
├── requirements.txt             ← Python dependencies
├── .env.example                 ← Database credentials template
│
├── sql/
│   ├── 01_create_schema.sql     ← PostgreSQL DDL (tables, indexes, DQ log)
│   ├── 02_seed_dimensions.sql   ← Desk & product reference data
│   └── 03_create_views.sql      ← Reporting views (Actual/Plan/Prior variance)
│
├── python/
│   ├── config.py                ← Central config: paths, desk definitions, stock→product mapping
│   ├── simulate_data.py         ← Build desk P&L from real market data
│   ├── validate.py              ← Data quality checks (4 validations)
│   ├── load_to_pg.py            ← Transform & load into PostgreSQL
│   ├── run_pipeline.py          ← End-to-end orchestrator
│   └── export_for_excel.py      ← Export views to CSV for Excel/Tableau
│
├── excel/
│   └── EXCEL_SETUP_GUIDE.md     ← Power Query, PivotTable & Solver instructions
│
├── tableau/
│   └── TABLEAU_DASHBOARD_GUIDE.md ← Dashboard specs, calc fields, color palette
│
├── data/                        ← Generated at runtime (git-ignored except source/)
│   ├── source/                  ← Real market datasets (input)
│   │   ├── macro_data_25yrs.csv
│   │   └── financial_timeseries_dataset.csv
│   ├── raw/                     ← Pipeline output CSVs (daily P&L, plan, FX, calendar)
│   └── processed/               ← Exported reporting views for Excel/Tableau
│
└── docs/
    └── PROJECT_WRITEUP.md       ← Full project description for resume/interviews
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- PostgreSQL 14+ (or use CSV-only mode for demo)
- Excel with Power Query (Microsoft 365 / Excel 2016+)
- Tableau Desktop or Tableau Public

### Setup

```bash
# 1. Clone / navigate to the project
cd "fp&a"

# 2. Create a virtual environment
python -m venv .venv
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure database (copy and edit .env)
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# 5. Create the database and schema
psql -U postgres -c "CREATE DATABASE fpa_reporting;"
psql -U postgres -d fpa_reporting -f sql/01_create_schema.sql
psql -U postgres -d fpa_reporting -f sql/02_seed_dimensions.sql
psql -U postgres -d fpa_reporting -f sql/03_create_views.sql
```

### Run the Pipeline

```bash
# Full pipeline: build data → validate → load
cd python
python run_pipeline.py --step all

# Or run individual steps:
python run_pipeline.py --step simulate    # Build P&L from real data
python run_pipeline.py --step validate    # Run DQ checks only
python run_pipeline.py --step load        # Load into PostgreSQL only

# Export views to CSV (for Excel/Tableau without live DB)
python export_for_excel.py
```

### CSV-Only Mode (No PostgreSQL Required)

If you just want to generate the data for Excel/Tableau without a database:

```bash
cd python
python simulate_data.py          # Creates CSVs in data/raw/
```

Then import the CSVs directly into Excel or Tableau (see guides in `excel/` and `tableau/`).

---

## 📊 Pipeline Output

Running `simulate_data.py` produces:

| File | Rows | Grain | Description |
|------|------|-------|-------------|
| `daily_pnl_{desk}.csv` × 4 | ~30,200 total | Desk × Product × Day | Daily gross/net P&L, notional, trades, RWA |
| `plan_budget.csv` | ~1,400 | Desk × Product × Month | Monthly budget targets |
| `prior_year_actuals.csv` | ~1,400 | Desk × Product × Month | YoY comparison baseline |
| `fx_rates.csv` | ~520 | Currency × Month | 6 currency pairs vs USD |
| `calendar.csv` | 1,889 | Day | Business-day calendar with fiscal metadata |

### Key Metrics

| Metric | Value |
|--------|-------|
| Date range | Apr 2018 – Jun 2025 (7+ years) |
| Trading days | 1,889 |
| Months | 87 |
| Desks | 4 |
| Products | 16 |
| Daily P&L range | -$1.6M to +$1.4M |

---

## 📊 Key Views

### `vw_monthly_variance` — Core Reporting View
| Metric | Description |
|--------|-------------|
| `actual_pnl` | Sum of daily net P&L for the month |
| `plan_pnl` | Monthly budget target |
| `prior_pnl` | Same month last year |
| `var_actual_vs_plan` | Actual − Plan ($) |
| `var_actual_vs_prior` | Actual − Prior Year ($) |
| `var_pct_vs_plan` | Variance as % of plan |
| `rorwa_annualized` | Return on Risk-Weighted Assets (annualized) |

### Desks & Products

| Desk | Products |
|------|----------|
| **Equities Cash** | US Cash Equities, Canadian Cash Equities, International Cash Eq, ETF Market Making |
| **Equity Derivatives** | Index Options, Single-Stock Options, Equity Swaps/TRS, Structured Notes |
| **Fixed Income Rates** | Government Bonds, Interest Rate Swaps, Repo/Financing, IG Credit |
| **FX & Commodities** | Spot FX, FX Forwards/NDFs, FX Options, Commodities (Energy/Metal) |

---

## 📈 Impact

| Metric | Before | After |
|--------|--------|-------|
| Month-end close time | ~16 hours (2 days) | ~2 hours |
| Reporting frequency | Monthly only | Daily dashboards + monthly close |
| Data refresh | Manual copy-paste | One-click / scheduled |
| Reconciliation errors | Frequent | Near-zero (automated DQ checks) |
| Source of truth | 5 disconnected workbooks | 1 PostgreSQL data model |
| Capital allocation | Subjective | Solver-optimized recommendation |

---

## 🎯 Resume Bullets

> **FP&A Reporting Automation** | *Excel (Power Query, PivotTables, Solver), SQL (PostgreSQL), Tableau, Python*

- **Engineered** a PostgreSQL data model and Python ETL pipeline to consolidate daily P&L, budget, and prior-year data across four global-markets trading desks, enabling automated Actual vs Plan vs Prior variance analysis and reducing month-end close reporting time by ~75%.
- **Built** self-refreshing Excel reports using Power Query connected to PostgreSQL, with PivotTables and an executive-summary template that replaced 16+ hours of manual VLOOKUP-driven processes with a one-click, sub-minute data refresh for FP&A and senior management.
- **Designed** a Tableau dashboard suite—including a daily desk P&L scorecard, a 12-month trend view, and a Return-on-RWA scatter plot—to provide desk heads and the Head of Markets with real-time visibility into trading performance versus plan and prior year.
- **Developed** a Solver-based capital-allocation optimizer in Excel that recommends next-month RWA distribution across desks to maximize expected P&L subject to firm-wide risk limits, directly supporting data-driven planning and scenario analysis for the monthly business review.

---

## 📝 Tech Stack

| Tool | Role in Project |
|------|----------------|
| **Python** (pandas, numpy, sqlalchemy) | Data transformation, quality checks, ETL pipeline |
| **PostgreSQL** | Centralized data model, reporting views, single source of truth |
| **Excel — Power Query** | Automated data refresh from PostgreSQL/CSV |
| **Excel — PivotTables** | Interactive variance analysis (desk/product/month drill-down) |
| **Excel — Solver** | Capital allocation optimization (LP: maximize P&L subject to RWA constraints) |
| **Tableau** | Daily interactive dashboards (scorecard, trend, RoRWA scatter) |

---

## 📄 License

This is a portfolio project built for educational and demonstration purposes.
Market data sourced from publicly available macroeconomic and financial timeseries datasets.
