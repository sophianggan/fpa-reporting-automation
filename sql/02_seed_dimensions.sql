-- ============================================================
-- FP&A Reporting Automation — Seed Dimension Data
-- Populates dim_desk and dim_product with reference data
-- (Calendar and FX rates are loaded via the Python pipeline
--  from real market data: Apr 2018 – Jun 2025)
-- ============================================================

-- Desks
INSERT INTO gm_fpa.dim_desk (desk_code, desk_name, desk_head, region) VALUES
    ('EQ_CASH',  'Equities Cash Trading',    'Sarah Chen',       'NAM'),
    ('EQ_DERIV', 'Equity Derivatives',        'Marcus Williams',  'NAM'),
    ('FI_RATES', 'Fixed Income Rates',         'James O''Brien',   'NAM'),
    ('FX_COMM',  'FX & Commodities',           'Priya Sharma',     'NAM');

-- Products
-- Equities Cash
INSERT INTO gm_fpa.dim_product (product_code, product_name, asset_class, desk_id) VALUES
    ('CASH_EQ_US',  'US Cash Equities',       'Equities',     (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='EQ_CASH')),
    ('CASH_EQ_CA',  'Canadian Cash Equities',  'Equities',     (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='EQ_CASH')),
    ('CASH_EQ_INT', 'International Cash Eq',   'Equities',     (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='EQ_CASH')),
    ('ETF_MM',      'ETF Market Making',       'Equities',     (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='EQ_CASH'));

-- Equity Derivatives
INSERT INTO gm_fpa.dim_product (product_code, product_name, asset_class, desk_id) VALUES
    ('INDEX_OPT', 'Index Options',            'Equities',     (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='EQ_DERIV')),
    ('SINGLE_OPT','Single-Stock Options',     'Equities',     (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='EQ_DERIV')),
    ('EQ_SWAP',   'Equity Swaps / TRS',       'Equities',     (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='EQ_DERIV')),
    ('STRUCT_NOTE','Structured Notes',         'Equities',     (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='EQ_DERIV'));

-- Fixed Income Rates
INSERT INTO gm_fpa.dim_product (product_code, product_name, asset_class, desk_id) VALUES
    ('GOVT_BOND', 'Government Bonds',         'Fixed Income', (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='FI_RATES')),
    ('IRS',       'Interest Rate Swaps',       'Fixed Income', (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='FI_RATES')),
    ('REPO',      'Repo / Financing',          'Fixed Income', (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='FI_RATES')),
    ('CREDIT',    'Investment Grade Credit',   'Fixed Income', (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='FI_RATES'));

-- FX & Commodities
INSERT INTO gm_fpa.dim_product (product_code, product_name, asset_class, desk_id) VALUES
    ('SPOT_FX',   'Spot FX',                  'FX',           (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='FX_COMM')),
    ('FX_FWD',    'FX Forwards / NDFs',        'FX',           (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='FX_COMM')),
    ('FX_OPT',    'FX Options',                'FX',           (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='FX_COMM')),
    ('CMDTY',     'Commodities (Energy/Metal)','Commodities',  (SELECT desk_id FROM gm_fpa.dim_desk WHERE desk_code='FX_COMM'));
