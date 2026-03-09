"""
load_to_pg.py — Transform and load data into PostgreSQL.

Reads validated CSVs from data/raw/ (generated from real market data),
maps them to the gm_fpa schema, and bulk-loads into PostgreSQL using
SQLAlchemy.

Steps:
  1. Load dimension data (calendar, FX rates)
  2. Map desk/product codes to their database IDs
  3. Transform daily P&L: map foreign keys, compute derived fields
  4. Load fact_daily_pnl (~30K rows), fact_plan (~1.4K), fact_prior_year (~1.4K)
  5. Log completion to dq_log
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
from config import (
    DESKS, RAW_DIR, PROCESSED_DIR, DB_SCHEMA,
    get_connection_string
)


def get_engine():
    """Create SQLAlchemy engine."""
    return create_engine(get_connection_string())


def load_calendar(engine) -> dict:
    """Load calendar CSV into dim_calendar and return date mapping."""
    print("\n  Loading calendar...")
    cal = pd.read_csv(RAW_DIR / "calendar.csv", parse_dates=["date_key"])
    cal["date_key"] = cal["date_key"].dt.date

    cal.to_sql(
        "dim_calendar", engine,
        schema=DB_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
    )
    print(f"    → {len(cal)} business days loaded into dim_calendar")
    return cal


def load_fx_rates(engine):
    """Load FX rates CSV into dim_fx_rate."""
    print("\n  Loading FX rates...")
    fx = pd.read_csv(RAW_DIR / "fx_rates.csv")

    fx.to_sql(
        "dim_fx_rate", engine,
        schema=DB_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
    )
    print(f"    → {len(fx)} FX rate rows loaded into dim_fx_rate")


def get_desk_product_maps(engine) -> tuple:
    """Fetch desk and product ID mappings from the database."""
    with engine.connect() as conn:
        desks = pd.read_sql(
            f"SELECT desk_id, desk_code FROM {DB_SCHEMA}.dim_desk", conn
        )
        products = pd.read_sql(
            f"SELECT product_id, product_code FROM {DB_SCHEMA}.dim_product", conn
        )

    desk_map = dict(zip(desks["desk_code"], desks["desk_id"]))
    product_map = dict(zip(products["product_code"], products["product_id"]))
    return desk_map, product_map


def load_daily_pnl(engine, desk_map: dict, product_map: dict):
    """Load all desk daily P&L files into fact_daily_pnl."""
    print("\n  Loading daily P&L...")
    total_rows = 0

    for desk_code in DESKS:
        path = RAW_DIR / f"daily_pnl_{desk_code}.csv"
        if not path.exists():
            print(f"    ⚠ Missing file: {path}")
            continue

        df = pd.read_csv(path, parse_dates=["date_key"])

        # Map to foreign key IDs
        df["desk_id"] = df["desk_code"].map(desk_map)
        df["product_id"] = df["product_code"].map(product_map)
        df["date_key"] = df["date_key"].dt.date

        # Validate mappings
        if df["desk_id"].isnull().any():
            bad = df[df["desk_id"].isnull()]["desk_code"].unique()
            print(f"    ⚠ Unmapped desk codes: {bad}")
            df = df.dropna(subset=["desk_id"])

        if df["product_id"].isnull().any():
            bad = df[df["product_id"].isnull()]["product_code"].unique()
            print(f"    ⚠ Unmapped product codes: {bad}")
            df = df.dropna(subset=["product_id"])

        # Select columns matching the table schema
        load_cols = [
            "date_key", "desk_id", "product_id",
            "gross_pnl_usd", "net_pnl_usd",
            "notional_traded_usd", "num_trades", "rwa_usd"
        ]
        df_load = df[load_cols].copy()
        df_load["desk_id"] = df_load["desk_id"].astype(int)
        df_load["product_id"] = df_load["product_id"].astype(int)

        df_load.to_sql(
            "fact_daily_pnl", engine,
            schema=DB_SCHEMA,
            if_exists="append",
            index=False,
            method="multi",
        )
        total_rows += len(df_load)
        print(f"    → {desk_code}: {len(df_load):,} rows loaded")

    print(f"    → Total: {total_rows:,} rows into fact_daily_pnl")


def load_plan(engine, desk_map: dict, product_map: dict):
    """Load plan/budget CSV into fact_plan."""
    print("\n  Loading plan/budget...")
    df = pd.read_csv(RAW_DIR / "plan_budget.csv")

    df["desk_id"] = df["desk_code"].map(desk_map)
    df["product_id"] = df["product_code"].map(product_map)

    load_cols = [
        "year_month", "desk_id", "product_id",
        "planned_net_pnl_usd", "planned_rwa_usd", "planned_notional_usd"
    ]
    df_load = df[load_cols].dropna(subset=["desk_id", "product_id"]).copy()
    df_load["desk_id"] = df_load["desk_id"].astype(int)
    df_load["product_id"] = df_load["product_id"].astype(int)

    df_load.to_sql(
        "fact_plan", engine,
        schema=DB_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
    )
    print(f"    → {len(df_load)} rows loaded into fact_plan")


def load_prior_year(engine, desk_map: dict, product_map: dict):
    """Load prior-year actuals CSV into fact_prior_year."""
    print("\n  Loading prior-year actuals...")
    df = pd.read_csv(RAW_DIR / "prior_year_actuals.csv")

    df["desk_id"] = df["desk_code"].map(desk_map)
    df["product_id"] = df["product_code"].map(product_map)

    load_cols = [
        "year_month", "desk_id", "product_id",
        "prior_net_pnl_usd", "prior_rwa_usd"
    ]
    df_load = df[load_cols].dropna(subset=["desk_id", "product_id"]).copy()
    df_load["desk_id"] = df_load["desk_id"].astype(int)
    df_load["product_id"] = df_load["product_id"].astype(int)

    df_load.to_sql(
        "fact_prior_year", engine,
        schema=DB_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
    )
    print(f"    → {len(df_load)} rows loaded into fact_prior_year")


def log_completion(engine, message: str):
    """Write a completion entry to the dq_log table."""
    with engine.connect() as conn:
        conn.execute(text(
            f"INSERT INTO {DB_SCHEMA}.dq_log (check_name, severity, details) "
            f"VALUES ('load_complete', 'INFO', :msg)"
        ), {"msg": message})
        conn.commit()


def main():
    print("=" * 60)
    print("FP&A Data Loader — Loading data into PostgreSQL")
    print("=" * 60)

    engine = get_engine()

    # Step 1: Load dimensions (calendar, FX)
    print("\n[Step 1] Loading dimension data...")
    load_calendar(engine)
    load_fx_rates(engine)

    # Step 2: Get ID maps for desk & product
    print("\n[Step 2] Fetching desk/product ID mappings...")
    desk_map, product_map = get_desk_product_maps(engine)
    print(f"    Desks:    {desk_map}")
    print(f"    Products: {product_map}")

    # Step 3: Load fact tables
    print("\n[Step 3] Loading fact tables...")
    load_daily_pnl(engine, desk_map, product_map)
    load_plan(engine, desk_map, product_map)
    load_prior_year(engine, desk_map, product_map)

    # Step 4: Log completion
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_completion(engine, f"Pipeline load completed at {ts}")
    print(f"\n{'=' * 60}")
    print(f"✓ Load complete at {ts}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
