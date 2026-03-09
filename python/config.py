"""
config.py — Central configuration for the FP&A Reporting Automation pipeline.

Uses environment variables (via .env file) for database credentials.
Provides constants for desk/product definitions, stock-to-product mappings,
and file paths used across the pipeline.

Data sources (bundled in data/source/):
  - macro_data_25yrs.csv            1,890 rows | Apr 2018 – Jun 2025
    Columns: Date, M2, 10Y Yield, Fed Funds, CPI, Inflation %, SOFR
  - financial_timeseries_dataset.csv  3,741 rows | 5 stocks × 30 lags + target
    Stocks: AAPL, GOOGL, MSFT, AMZN, JPM (standardized returns)
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env if present ──────────────────────────────────────
load_dotenv()

# ── Project paths ─────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
SOURCE_DIR = DATA_DIR / "source"
LOG_DIR = PROJECT_ROOT / "logs"

# Create dirs if they don't exist
for d in [DATA_DIR, RAW_DIR, PROCESSED_DIR, SOURCE_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── PostgreSQL connection ─────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5432)),
    "database": os.getenv("PG_DATABASE", "fpa_reporting"),
    "user":     os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}

DB_SCHEMA = "gm_fpa"

def get_connection_string():
    """Return a SQLAlchemy-compatible PostgreSQL connection string."""
    c = DB_CONFIG
    return f"postgresql://{c['user']}:{c['password']}@{c['host']}:{c['port']}/{c['database']}"


# ── Desk definitions ──────────────────────────────────────────
# Reference data for dimensions (loaded into PostgreSQL dim tables).
# P&L generation now driven by STOCK_TO_PRODUCT_MAP below, not these params.

DESKS = {
    "EQ_CASH":  {"desk_name": "Equities Cash Trading",  "desk_head": "Sarah Chen",      "region": "NAM"},
    "EQ_DERIV": {"desk_name": "Equity Derivatives",      "desk_head": "Marcus Williams", "region": "NAM"},
    "FI_RATES": {"desk_name": "Fixed Income Rates",      "desk_head": "James O'Brien",   "region": "NAM"},
    "FX_COMM":  {"desk_name": "FX & Commodities",        "desk_head": "Priya Sharma",    "region": "NAM"},
}

# All products across all desks (for validation reference)
ALL_PRODUCTS = {
    "CASH_EQ_US", "CASH_EQ_CA", "CASH_EQ_INT", "ETF_MM",
    "INDEX_OPT", "SINGLE_OPT", "EQ_SWAP", "STRUCT_NOTE",
    "GOVT_BOND", "IRS", "REPO", "CREDIT",
    "SPOT_FX", "FX_FWD", "FX_OPT", "CMDTY",
}

# ── Real data source files ─────────────────────────────────────
# Bundled in data/source/ (or override via env vars)
MACRO_DATA_PATH = Path(os.getenv(
    "MACRO_CSV",
    str(SOURCE_DIR / "macro_data_25yrs.csv")
))
FINANCIAL_TS_PATH = Path(os.getenv(
    "FINANCIAL_TS_CSV",
    str(SOURCE_DIR / "financial_timeseries_dataset.csv")
))

# ── Pipeline parameters ───────────────────────────────────────
FUNDING_CHARGE_RATE = 0.05      # 5 bps of notional as internal funding charge

# Plan is set as prior_year * (1 + growth_target)
PLAN_GROWTH_TARGET = 0.05  # 5% YoY growth embedded in budget

# Seasonal volume multipliers (1.0 = normal)
SEASONAL_FACTORS = {
    1: 1.00, 2: 1.00, 3: 1.05, 4: 1.00,
    5: 0.95, 6: 1.00, 7: 0.90, 8: 0.75,
    9: 1.05, 10: 1.10, 11: 1.05, 12: 0.70,
}

# FX currencies derived from macro data
FX_CURRENCIES = ["CAD", "EUR", "GBP", "JPY", "CHF", "AUD"]

# ── Mapping: financial timeseries stocks → desks/products ─────
# Each stock's t-0 standardized return drives P&L for mapped products.
# The mapping assigns realistic dollar-scale and trade-count parameters.
STOCK_TO_PRODUCT_MAP = {
    # AAPL → Equities Cash (US large-cap flow proxy)
    "AAPL": {
        "desk_code": "EQ_CASH",
        "products": {
            "CASH_EQ_US":  {"weight": 0.50, "pnl_scale": 400_000, "trades_range": (300, 800),
                            "notional_range": (50e6, 200e6), "rwa_base": 250e6},
            "ETF_MM":      {"weight": 0.50, "pnl_scale": 250_000, "trades_range": (200, 600),
                            "notional_range": (80e6, 300e6), "rwa_base": 80e6},
        }
    },
    # GOOGL → Equity Derivatives (tech vol / options proxy)
    "GOOGL": {
        "desk_code": "EQ_DERIV",
        "products": {
            "INDEX_OPT":   {"weight": 0.40, "pnl_scale": 500_000, "trades_range": (30, 80),
                            "notional_range": (100e6, 500e6), "rwa_base": 400e6},
            "SINGLE_OPT":  {"weight": 0.35, "pnl_scale": 350_000, "trades_range": (20, 60),
                            "notional_range": (50e6, 200e6), "rwa_base": 300e6},
            "STRUCT_NOTE": {"weight": 0.25, "pnl_scale": 250_000, "trades_range": (5, 20),
                            "notional_range": (30e6, 150e6), "rwa_base": 200e6},
        }
    },
    # MSFT → also Equities (international / Canadian proxy)
    "MSFT": {
        "desk_code": "EQ_CASH",
        "products": {
            "CASH_EQ_CA":  {"weight": 0.55, "pnl_scale": 200_000, "trades_range": (100, 400),
                            "notional_range": (20e6, 80e6), "rwa_base": 120e6},
            "CASH_EQ_INT": {"weight": 0.45, "pnl_scale": 250_000, "trades_range": (50, 200),
                            "notional_range": (10e6, 60e6), "rwa_base": 100e6},
        }
    },
    # AMZN → FX & Commodities (high-vol, global trade proxy)
    "AMZN": {
        "desk_code": "FX_COMM",
        "products": {
            "SPOT_FX":     {"weight": 0.30, "pnl_scale": 350_000, "trades_range": (100, 500),
                            "notional_range": (200e6, 1e9), "rwa_base": 150e6},
            "FX_FWD":      {"weight": 0.25, "pnl_scale": 300_000, "trades_range": (30, 120),
                            "notional_range": (100e6, 500e6), "rwa_base": 200e6},
            "FX_OPT":      {"weight": 0.20, "pnl_scale": 350_000, "trades_range": (10, 50),
                            "notional_range": (50e6, 300e6), "rwa_base": 250e6},
            "CMDTY":       {"weight": 0.25, "pnl_scale": 400_000, "trades_range": (20, 80),
                            "notional_range": (30e6, 200e6), "rwa_base": 180e6},
        }
    },
    # JPM → Fixed Income Rates (bank / rates proxy)
    "JPM": {
        "desk_code": "FI_RATES",
        "products": {
            "GOVT_BOND":   {"weight": 0.30, "pnl_scale": 450_000, "trades_range": (50, 150),
                            "notional_range": (500e6, 2e9), "rwa_base": 500e6},
            "IRS":         {"weight": 0.30, "pnl_scale": 600_000, "trades_range": (20, 80),
                            "notional_range": (1e9, 5e9), "rwa_base": 600e6},
            "REPO":        {"weight": 0.20, "pnl_scale": 150_000, "trades_range": (30, 100),
                            "notional_range": (500e6, 3e9), "rwa_base": 200e6},
            "CREDIT":      {"weight": 0.20, "pnl_scale": 350_000, "trades_range": (15, 50),
                            "notional_range": (100e6, 500e6), "rwa_base": 350e6},
        }
    },
}

# Equity Swaps (EQ_SWAP) is driven by a blend of GOOGL + MSFT
EQ_SWAP_CONFIG = {
    "desk_code": "EQ_DERIV",
    "product_code": "EQ_SWAP",
    "blend_stocks": ["GOOGL", "MSFT"],
    "blend_weights": [0.6, 0.4],
    "pnl_scale": 250_000,
    "trades_range": (10, 40),
    "notional_range": (200e6, 800e6),
    "rwa_base": 350e6,
}
