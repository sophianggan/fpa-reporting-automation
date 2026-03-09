-- ============================================================
-- FP&A Reporting Automation — Reporting Views
-- Core views that power Excel Power Query and Tableau
-- ============================================================

-- ============================================================
-- VIEW 1: Monthly Variance — full detail at desk/product/month
-- This is the primary view for Actual vs Plan vs Prior analysis
-- ============================================================
CREATE OR REPLACE VIEW gm_fpa.vw_monthly_variance AS
SELECT
    c.year_month,
    c.fiscal_quarter,
    c.fiscal_year,
    d.desk_id,
    d.desk_code,
    d.desk_name,
    d.desk_head,
    d.region,
    p.product_id,
    p.product_code,
    p.product_name,
    p.asset_class,

    -- ── Actuals (aggregated daily → monthly) ──
    SUM(a.gross_pnl_usd)                             AS actual_gross_pnl,
    SUM(a.net_pnl_usd)                               AS actual_pnl,
    SUM(a.rwa_usd)                                    AS actual_rwa,
    SUM(a.notional_traded_usd)                        AS actual_notional,
    SUM(a.num_trades)                                 AS actual_trades,

    -- ── Plan ──
    MAX(pl.planned_net_pnl_usd)                       AS plan_pnl,
    MAX(pl.planned_rwa_usd)                           AS plan_rwa,
    MAX(pl.planned_notional_usd)                      AS plan_notional,

    -- ── Prior Year ──
    MAX(pr.prior_net_pnl_usd)                         AS prior_pnl,
    MAX(pr.prior_rwa_usd)                             AS prior_rwa,

    -- ── Variances ──
    SUM(a.net_pnl_usd) - MAX(pl.planned_net_pnl_usd) AS var_actual_vs_plan,
    SUM(a.net_pnl_usd) - MAX(pr.prior_net_pnl_usd)   AS var_actual_vs_prior,
    MAX(pl.planned_net_pnl_usd) - MAX(pr.prior_net_pnl_usd)
                                                       AS var_plan_vs_prior,

    -- ── Variance % ──
    CASE WHEN MAX(pl.planned_net_pnl_usd) <> 0
         THEN ROUND(
              (SUM(a.net_pnl_usd) - MAX(pl.planned_net_pnl_usd))
              / ABS(MAX(pl.planned_net_pnl_usd)) * 100, 2)
         ELSE NULL END                                 AS var_pct_vs_plan,

    CASE WHEN MAX(pr.prior_net_pnl_usd) <> 0
         THEN ROUND(
              (SUM(a.net_pnl_usd) - MAX(pr.prior_net_pnl_usd))
              / ABS(MAX(pr.prior_net_pnl_usd)) * 100, 2)
         ELSE NULL END                                 AS var_pct_vs_prior,

    -- ── Return on RWA (annualized) ──
    CASE WHEN SUM(a.rwa_usd) > 0
         THEN ROUND(SUM(a.net_pnl_usd) * 12 / SUM(a.rwa_usd) * 100, 2)
         ELSE NULL END                                 AS rorwa_annualized

FROM      gm_fpa.fact_daily_pnl  a
JOIN      gm_fpa.dim_calendar    c  ON a.date_key    = c.date_key
JOIN      gm_fpa.dim_desk        d  ON a.desk_id     = d.desk_id
JOIN      gm_fpa.dim_product     p  ON a.product_id  = p.product_id
LEFT JOIN gm_fpa.fact_plan       pl ON pl.year_month  = c.year_month
                                    AND pl.desk_id    = a.desk_id
                                    AND pl.product_id = a.product_id
LEFT JOIN gm_fpa.fact_prior_year pr ON pr.year_month  = c.year_month
                                    AND pr.desk_id    = a.desk_id
                                    AND pr.product_id = a.product_id
GROUP BY
    c.year_month, c.fiscal_quarter, c.fiscal_year,
    d.desk_id, d.desk_code, d.desk_name, d.desk_head, d.region,
    p.product_id, p.product_code, p.product_name, p.asset_class;


-- ============================================================
-- VIEW 2: Desk Summary — rolled up to desk/month for exec pack
-- ============================================================
CREATE OR REPLACE VIEW gm_fpa.vw_desk_summary AS
SELECT
    year_month,
    fiscal_quarter,
    fiscal_year,
    desk_id,
    desk_code,
    desk_name,
    desk_head,
    region,
    SUM(actual_pnl)            AS total_actual_pnl,
    SUM(plan_pnl)              AS total_plan_pnl,
    SUM(prior_pnl)             AS total_prior_pnl,
    SUM(var_actual_vs_plan)    AS total_var_vs_plan,
    SUM(var_actual_vs_prior)   AS total_var_vs_prior,
    SUM(actual_rwa)            AS total_rwa,
    SUM(actual_notional)       AS total_notional,
    SUM(actual_trades)         AS total_trades,
    CASE WHEN SUM(actual_rwa) > 0
         THEN ROUND(SUM(actual_pnl) * 12 / SUM(actual_rwa) * 100, 2)
         ELSE NULL END         AS desk_rorwa_annualized
FROM gm_fpa.vw_monthly_variance
GROUP BY
    year_month, fiscal_quarter, fiscal_year,
    desk_id, desk_code, desk_name, desk_head, region;


-- ============================================================
-- VIEW 3: YTD Summary — cumulative year-to-date by desk
-- Used for YTD gap-to-plan tracking in Solver optimizer
-- ============================================================
CREATE OR REPLACE VIEW gm_fpa.vw_ytd_summary AS
SELECT
    fiscal_year,
    desk_code,
    desk_name,
    SUM(actual_pnl)          AS ytd_actual_pnl,
    SUM(plan_pnl)            AS ytd_plan_pnl,
    SUM(prior_pnl)           AS ytd_prior_pnl,
    SUM(var_actual_vs_plan)  AS ytd_var_vs_plan,
    SUM(actual_rwa)          AS ytd_rwa,
    CASE WHEN SUM(actual_rwa) > 0
         THEN ROUND(SUM(actual_pnl) * 12 / SUM(actual_rwa) * 100, 2)
         ELSE NULL END       AS ytd_rorwa
FROM gm_fpa.vw_monthly_variance
GROUP BY fiscal_year, desk_code, desk_name;


-- ============================================================
-- VIEW 4: Daily P&L with dimensions — for Tableau daily dashboard
-- ============================================================
CREATE OR REPLACE VIEW gm_fpa.vw_daily_pnl_detail AS
SELECT
    a.date_key,
    c.year_month,
    c.fiscal_quarter,
    c.business_day_of_month,
    c.total_business_days,
    d.desk_code,
    d.desk_name,
    d.region,
    p.product_code,
    p.product_name,
    p.asset_class,
    a.gross_pnl_usd,
    a.net_pnl_usd,
    a.notional_traded_usd,
    a.num_trades,
    a.rwa_usd
FROM gm_fpa.fact_daily_pnl a
JOIN gm_fpa.dim_calendar   c ON a.date_key   = c.date_key
JOIN gm_fpa.dim_desk       d ON a.desk_id    = d.desk_id
JOIN gm_fpa.dim_product    p ON a.product_id = p.product_id;
