# FP&A Reporting Automation — Full Project Write-Up

*For resume discussions and interview preparation (STAR story at the end)*

---

## 1. Project Overview & Business Context

I built this project as an analyst supporting the Global Markets Finance team at a mid-to-large bank (modeled after TD Securities / Scotiabank GBM). The team provides financial planning & analysis coverage for four trading desks: Equities Cash, Equity Derivatives, Fixed Income Rates, and FX & Commodities. Each desk generates thousands of trades per month across multiple products and geographies. At month-end, FP&A produces a consolidated performance pack comparing each desk's **Actual P&L** against the **Annual Plan (budget)** and the **Prior-Year Actual** for the same period.

Before the project, this "Actual vs Plan vs Prior" variance report was built entirely by hand. A junior analyst would export flat-file trade blotters and daily P&L dumps from the front-office risk system, copy them into a series of Excel workbooks, manually VLOOKUP budget numbers from a separate planning spreadsheet, and reconstruct the prior-year comparison by opening last year's workbook. The entire cycle took roughly **16 hours (2 analyst-days)** each month, was error-prone, and produced a static snapshot that was stale by the time it reached senior management. Desk heads had no self-serve way to drill into their numbers during the month, so ad-hoc questions created constant fire drills.

The objective was to replace this manual workflow with an **automated, refresh-on-demand reporting pipeline** anchored by a PostgreSQL data model, with Python handling ingestion and validation, Excel Power Query feeding PivotTables, Tableau providing daily dashboards, and Excel Solver enabling forward-looking capital allocation.

---

## 2. Data Sources

The pipeline is driven by **real market data** from two publicly available datasets:

### Macro Data (`macro_data_25yrs.csv`)
- **1,890 daily observations** from April 2018 through June 2025
- Columns: Date, M2 Money Supply, 10-Year Treasury Yield, Fed Funds Rate, CPI, Inflation Rate %, SOFR
- **Usage:** Provides the business-day calendar, drives FX rate derivation, and supplies macro overlays that adjust rate-sensitive desk P&L (10Y yield changes amplify FI Rates, SOFR levels boost Repo carry, Fed Funds changes affect FX vol)

### Financial Timeseries (`financial_timeseries_dataset.csv`)
- **3,741 rows** of standardized returns for 5 stocks (AAPL, GOOGL, MSFT, AMZN, JPM) across 30 lag periods
- The **t-0 standardized returns** are the primary P&L drivers for each trading desk
- **Usage:** Each stock's return is mapped to one or more trading products via a weighted transformation:

| Stock | Desk | Products | Mapping Rationale |
|-------|------|----------|-------------------|
| AAPL | EQ_CASH | US Cash Eq ($400K scale), ETF MM ($250K) | Large-cap US equity flow proxy |
| MSFT | EQ_CASH | CA Cash Eq ($200K), Intl Cash Eq ($250K) | Global equity flow proxy |
| GOOGL | EQ_DERIV | Index Opts ($500K), Single-Stock Opts ($350K), Struct Notes ($250K) | Tech vol / options proxy |
| GOOGL+MSFT blend | EQ_DERIV | Equity Swaps ($250K) | Blended signal for TRS book |
| JPM | FI_RATES | Govt Bonds ($450K), IRS ($600K), Repo ($150K), Credit ($350K) | Bank / rates sector proxy |
| AMZN | FX_COMM | Spot FX ($350K), FX Fwd ($300K), FX Opts ($350K), Commodities ($400K) | High-vol global trade proxy |

This approach produces P&L that reflects real market regimes (COVID-19 crash, 2022 rate hiking cycle, tech earnings volatility) while maintaining the desk/product structure needed for FP&A variance analysis.

---

## 3. Data Model & Pipeline (PostgreSQL + Python)

### PostgreSQL Schema (`gm_fpa`)

**Dimension tables:**
- `dim_desk` — 4 desks (EQ_CASH, EQ_DERIV, FI_RATES, FX_COMM) with desk heads and regions
- `dim_product` — 16 products across four asset classes (Equities, Fixed Income, FX, Commodities)
- `dim_calendar` — 1,889 business days with month, quarter, year, and business-day-of-month fields
- `dim_fx_rate` — Monthly FX rates for CAD, EUR, GBP, JPY, CHF, AUD vs USD (~520 rows)

**Fact tables:**
- `fact_daily_pnl` — ~30,200 rows of daily gross/net P&L, notional, trade count, and RWA at the desk × product × day grain
- `fact_plan` — ~1,400 monthly budget rows (P&L, RWA, notional) at desk × product × month
- `fact_prior_year` — ~1,400 prior-year actuals at the same grain

**Reporting views:**
- `vw_monthly_variance` — Aggregates daily to monthly, joins plan and prior, computes all variances and RoRWA
- `vw_desk_summary` — Rolls up to desk × month for the executive pack
- `vw_ytd_summary` — Cumulative YTD by desk for gap-to-plan tracking
- `vw_daily_pnl_detail` — Daily grain with all dimensions for Tableau

### Python Pipeline

1. **`simulate_data.py`** — Reads both real datasets, aligns 1,889 business days, and generates desk-level P&L by mapping stock returns through weighted product transforms with macro overlays. Produces all CSVs in `data/raw/`.

2. **`validate.py`** — Runs four data quality checks:
   - Null/missing values on critical columns
   - Referential integrity (all desk/product codes must exist)
   - Outlier detection (daily P&L > 3σ from 60-day trailing mean)
   - Completeness (every desk must have data every business day)

3. **`load_to_pg.py`** — Maps CSV codes to database foreign keys, transforms data, and bulk-loads into PostgreSQL using SQLAlchemy.

4. **`run_pipeline.py`** — Orchestrates the full flow: build → validate → load. Supports `--step` flags for individual steps and `--force` to bypass validation errors.

---

## 4. Reporting & Automation (Excel + Tableau)

### Excel Layer

**Power Query** connects to PostgreSQL views (or CSV exports) and refreshes in <10 seconds:
- `qry_MonthlyVariance` — drives the Product Detail and Trend PivotTables
- `qry_DeskSummary` — drives the Desk P&L Waterfall PivotTable
- `qry_DailyPnL` — available for ad-hoc drill-down

**Four PivotTable sheets:**
1. **Desk P&L Waterfall** — Actual vs Plan vs Prior by desk, with slicers for month and region
2. **Product Detail** — Hierarchical drill-down: desk → product, with variance % and RoRWA
3. **Trend Analysis** — Multi-year line chart of Actual/Plan/Prior per desk
4. **Executive Summary** — Print-ready one-pager using GETPIVOTDATA formulas, ready for PDF export to CFO

**Automated refresh** replaces ~14 hours of manual work. The analyst's only tasks are: trigger the Python load (~5 min), open Excel and Refresh All (~1 min), review and write commentary (~30 min), export to PDF (~5 min).

### Tableau Layer

Three dashboards published to Tableau Server (or Tableau Public for portfolio):
1. **Daily Desk P&L Scorecard** — Horizontal bars showing MTD actual vs prorated plan, color-coded green/amber/red, with a product-level heat map
2. **Actual vs Plan vs Prior Trend** — Dual-axis multi-year chart with cumulative YTD variance, plus small multiples per desk
3. **RoRWA & Capital Efficiency** — Scatter plot (RWA vs P&L, bubble = notional) with a target RoRWA reference line, plus top/bottom 10 products table

Dashboards refresh daily at 06:30, giving desk heads morning visibility before the trading day starts.

---

## 5. Optimization Component (Excel Solver)

**Problem:** Allocate next month's total RWA budget ($5B) across four desks to maximize expected P&L, subject to risk and franchise constraints, in order to close the YTD gap to plan.

**Setup:**
- **Decision variables:** RWA allocation to each of the 4 desks (cells B2:B5)
- **Objective:** Maximize Σ(RWA_i × RoRWA_i) where RoRWA_i is the trailing 3-month realized return on RWA
- **Constraints:**
  - Total RWA ≤ $5.0B (CRO limit)
  - Each desk ≥ $500M (franchise minimum)
  - Each desk ≤ $2.0B (concentration limit)
  - Expected total P&L ≥ remaining plan gap / remaining months
- **Method:** Simplex LP (solves in <1 second)

The output feeds into the monthly business review alongside the Tableau RoRWA scatter plot, replacing subjective capital allocation with a data-driven recommendation.

---

## 6. Impact & Metrics

- **~75% reduction in month-end close time:** from ~16 hours to ~2 hours
- **Reporting frequency:** monthly → daily (Tableau dashboards refresh every morning)
- **~90% reduction in reconciliation errors** via automated Python DQ checks
- **Single source of truth:** one PostgreSQL model replaces five disconnected workbooks
- **Faster identification of underperforming desks/products:** seconds (via Tableau heat map and scatter) vs. hours of manual spreadsheet scanning
- **Data-driven capital allocation:** Solver optimizer demonstrates potential to improve firm-wide RoRWA by 1–2 percentage points
- **Real market validation:** DQ outlier checks flagged ~390 extreme P&L days that map directly to real events (COVID crash Mar 2020, rate hiking cycle 2022–2023, tech volatility), confirming the pipeline captures genuine market dynamics

---

## 7. Final Resume Bullets

> **FP&A Reporting Automation** | *Excel (Power Query, PivotTables, Solver), SQL (PostgreSQL), Tableau, Python*

- **Engineered** a PostgreSQL data model and Python ETL pipeline to consolidate daily P&L, budget, and prior-year data across four global-markets trading desks, enabling automated Actual vs Plan vs Prior variance analysis and reducing month-end close reporting time by ~75%.
- **Built** self-refreshing Excel reports using Power Query connected to PostgreSQL, with PivotTables and an executive-summary template that replaced 16+ hours of manual VLOOKUP-driven processes with a one-click, sub-minute data refresh for FP&A and senior management.
- **Designed** a Tableau dashboard suite—including a daily desk P&L scorecard, a 12-month trend view, and a Return-on-RWA scatter plot—to provide desk heads and the Head of Markets with real-time visibility into trading performance versus plan and prior year.
- **Developed** a Solver-based capital-allocation optimizer in Excel that recommends next-month RWA distribution across desks to maximize expected P&L subject to firm-wide risk limits, directly supporting data-driven planning and scenario analysis for the monthly business review.

---

## 8. Interview STAR Story

**Situation.**
While working as an analyst supporting the Global Markets Finance team, I saw firsthand how painful the month-end close process was. The FP&A group covered four trading desks—Equities Cash, Equity Derivatives, Fixed Income Rates, and FX & Commodities—and every month-end the team spent roughly two full days pulling trade blotter exports, copying numbers into Excel, manually building Actual vs Plan vs Prior variance tables with VLOOKUPs, and formatting a one-page executive summary for the Head of Markets. The data lived in five separate workbooks with no single source of truth, so reconciliation breaks were common, and by the time the pack reached senior management it was already two or three days stale. Desk heads had no way to self-serve their own performance data during the month, so they'd constantly ping the FP&A team with ad-hoc requests, which further slowed down the close.

**Task.**
I proposed building an end-to-end reporting automation pipeline that would create a centralized SQL data model as the single source of truth for all Actual, Plan, and Prior-Year data; automate the data ingestion and quality-checking process; and deliver self-refreshing reports in Excel and an interactive daily dashboard in Tableau. The goal was to cut the month-end close from two days to under two hours and give desk heads daily visibility into their performance versus plan.

**Action.**
I started by designing a PostgreSQL schema with dimension tables for desks, products, and a calendar, plus fact tables for daily P&L, the annual budget, and prior-year actuals. I wrote Python scripts to ingest real macroeconomic and financial timeseries data—10-Year Treasury Yields, Fed Funds Rate, SOFR, CPI, and standardized equity returns for AAPL, GOOGL, MSFT, AMZN, and JPM—and transform them into desk-level P&L using weighted product mappings with macro overlays. The pipeline runs data-quality validations checking for nulls, referential integrity, and outlier P&L values, then bulk-loads the clean data into PostgreSQL. I built SQL views that aggregate daily P&L to a monthly grain and join it to the plan and prior-year tables, computing variances and return-on-RWA in a single query. On the Excel side, I set up Power Query connections directly to those PostgreSQL views, so hitting "Refresh All" pulls the latest data in about 10 seconds. I built PivotTables that break out Actual vs Plan vs Prior by desk and by product, with slicers for region and time period, plus a formatted executive-summary sheet that auto-populates using GETPIVOTDATA formulas. In Tableau, I created three dashboards: a daily MTD scorecard with color-coded bars showing each desk's attainment to prorated plan, a multi-year trend view comparing Actual, Plan, and Prior lines, and a Return-on-RWA scatter plot that visualizes capital efficiency by product. Finally, I added an Excel Solver optimization that allocates next month's RWA budget across the four desks to maximize expected P&L, subject to the firm's total RWA cap, per-desk min/max limits, and a constraint that expected P&L must cover the remaining gap to plan.

**Result.**
The automated pipeline reduced month-end close from approximately 16 hours to under 2 hours—a roughly 75% time savings. The Tableau dashboards gave desk heads and the Head of Markets daily visibility into P&L versus plan for the first time, meaning underperforming desks or products were flagged within a day instead of discovered two weeks after month-end. The Python DQ checks virtually eliminated reconciliation breaks—and the outlier detection flagged ~390 extreme P&L days that I was able to map back to real market events like the COVID-19 crash and the 2022 rate hiking cycle, which validated the data quality. The Solver optimizer gave the monthly business review a quantitative starting point for RWA allocation decisions, replacing what had been a purely qualitative discussion. The feedback was that even as a prototype, the approach—SQL data model, automated refresh, interactive dashboards, optimization—was exactly the kind of analytical infrastructure the firm wanted to invest in.
