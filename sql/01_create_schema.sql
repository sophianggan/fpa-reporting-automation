-- ============================================================
-- FP&A Reporting Automation — PostgreSQL Schema
-- Creates the gm_fpa schema and all dimension/fact tables
--
-- Data: 7+ years of real market data (Apr 2018 – Jun 2025)
--       sourced from macro_data_25yrs.csv and
--       financial_timeseries_dataset.csv
-- ============================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS gm_fpa;

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

-- Desk dimension
CREATE TABLE gm_fpa.dim_desk (
    desk_id     SERIAL PRIMARY KEY,
    desk_code   VARCHAR(10)  NOT NULL UNIQUE,
    desk_name   VARCHAR(100) NOT NULL,
    desk_head   VARCHAR(100),
    region      VARCHAR(20)  NOT NULL  -- NAM, EMEA, APAC
);

-- Product dimension
CREATE TABLE gm_fpa.dim_product (
    product_id    SERIAL PRIMARY KEY,
    product_code  VARCHAR(20)  NOT NULL UNIQUE,
    product_name  VARCHAR(100) NOT NULL,
    asset_class   VARCHAR(30)  NOT NULL,  -- Equities, Fixed Income, FX, Commodities
    desk_id       INT          NOT NULL REFERENCES gm_fpa.dim_desk(desk_id)
);

-- Calendar / date dimension
CREATE TABLE gm_fpa.dim_calendar (
    date_key              DATE PRIMARY KEY,
    year_month            CHAR(7)   NOT NULL,   -- '2026-01'
    fiscal_quarter        CHAR(6)   NOT NULL,   -- '2026Q1'
    fiscal_year           INT       NOT NULL,
    is_month_end          BOOLEAN   NOT NULL DEFAULT FALSE,
    business_day_of_month INT       NOT NULL,
    total_business_days   INT       NOT NULL     -- total biz days in that month
);

-- FX rates (month-end snapshot for multi-currency translation)
CREATE TABLE gm_fpa.dim_fx_rate (
    rate_id     SERIAL PRIMARY KEY,
    year_month  CHAR(7)       NOT NULL,
    ccy_from    CHAR(3)       NOT NULL,
    ccy_to      CHAR(3)       NOT NULL DEFAULT 'USD',
    rate        NUMERIC(12,6) NOT NULL,
    UNIQUE (year_month, ccy_from, ccy_to)
);

-- ============================================================
-- FACT TABLES
-- ============================================================

-- Daily P&L — actual trading performance at desk/product/day grain
-- ~30,000 rows covering 1,889 trading days (Apr 2018 – Jun 2025)
CREATE TABLE gm_fpa.fact_daily_pnl (
    pnl_id              SERIAL PRIMARY KEY,
    date_key            DATE          NOT NULL REFERENCES gm_fpa.dim_calendar(date_key),
    desk_id             INT           NOT NULL REFERENCES gm_fpa.dim_desk(desk_id),
    product_id          INT           NOT NULL REFERENCES gm_fpa.dim_product(product_id),
    gross_pnl_usd       NUMERIC(18,2) NOT NULL,
    net_pnl_usd         NUMERIC(18,2) NOT NULL,
    notional_traded_usd  NUMERIC(18,2) NOT NULL DEFAULT 0,
    num_trades           INT           NOT NULL DEFAULT 0,
    rwa_usd             NUMERIC(18,2) NOT NULL DEFAULT 0
);

CREATE INDEX idx_daily_pnl_date   ON gm_fpa.fact_daily_pnl(date_key);
CREATE INDEX idx_daily_pnl_desk   ON gm_fpa.fact_daily_pnl(desk_id);
CREATE INDEX idx_daily_pnl_prod   ON gm_fpa.fact_daily_pnl(product_id);

-- Annual plan / budget at desk/product/month grain
-- ~1,400 rows (87 months × 16 products)
CREATE TABLE gm_fpa.fact_plan (
    plan_id              SERIAL PRIMARY KEY,
    year_month           CHAR(7)       NOT NULL,
    desk_id              INT           NOT NULL REFERENCES gm_fpa.dim_desk(desk_id),
    product_id           INT           NOT NULL REFERENCES gm_fpa.dim_product(product_id),
    planned_net_pnl_usd  NUMERIC(18,2) NOT NULL,
    planned_rwa_usd      NUMERIC(18,2) NOT NULL DEFAULT 0,
    planned_notional_usd NUMERIC(18,2) NOT NULL DEFAULT 0,
    UNIQUE (year_month, desk_id, product_id)
);

-- Prior-year actuals at desk/product/month grain
-- ~1,400 rows (same grain as plan)
-- year_month holds the CURRENT-YEAR month this maps to
-- (e.g., row with year_month='2026-01' stores Jan 2025 actual P&L)
CREATE TABLE gm_fpa.fact_prior_year (
    prior_id           SERIAL PRIMARY KEY,
    year_month         CHAR(7)       NOT NULL,
    desk_id            INT           NOT NULL REFERENCES gm_fpa.dim_desk(desk_id),
    product_id         INT           NOT NULL REFERENCES gm_fpa.dim_product(product_id),
    prior_net_pnl_usd  NUMERIC(18,2) NOT NULL,
    prior_rwa_usd      NUMERIC(18,2) NOT NULL DEFAULT 0,
    UNIQUE (year_month, desk_id, product_id)
);

-- Data quality log
CREATE TABLE gm_fpa.dq_log (
    log_id       SERIAL PRIMARY KEY,
    run_ts       TIMESTAMP NOT NULL DEFAULT NOW(),
    check_name   VARCHAR(100) NOT NULL,
    severity     VARCHAR(10)  NOT NULL,  -- ERROR, WARNING, INFO
    details      TEXT,
    row_count    INT DEFAULT 0
);
