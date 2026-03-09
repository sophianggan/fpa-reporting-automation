"""
simulate_data.py — Build FP&A desk-level P&L data from real market datasets.

Reads two real-world datasets (bundled in data/source/):

  1. macro_data_25yrs.csv         1,890 rows | Apr 2018 – Jun 2025
     Columns: Date, M2 Money Supply, 10Y Treasury Yield, Fed Funds Rate,
              CPI, Inflation Rate %, SOFR
     Usage:   Business-day calendar, FX rate derivation, and
              macro-overlay adjustments for rate-sensitive desks.

  2. financial_timeseries_dataset.csv   3,741 rows | 5 stocks × 30 lags
     Columns: AAPL/GOOGL/MSFT/AMZN/JPM at t-0 through t-29 + binary target
     Usage:   The t-0 standardized returns are the PRIMARY P&L DRIVERS
              for each trading desk/product.

Stock-to-Desk mapping:
  AAPL  returns → EQ_CASH   (US Cash Equities, ETF Market Making)
  MSFT  returns → EQ_CASH   (Canadian Cash Eq, International Cash Eq)
  GOOGL returns → EQ_DERIV  (Index Opts, Single-Stock Opts, Structured Notes)
  GOOGL+MSFT   → EQ_DERIV  (Equity Swaps — blended signal)
  JPM   returns → FI_RATES  (Govt Bonds, IRS, Repo, Credit)
  AMZN  returns → FX_COMM   (Spot FX, FX Fwd, FX Opts, Commodities)

Macro overlay:
  - 10Y yield changes amplify/dampen FI Rates desk P&L
  - SOFR level scales Repo desk P&L (carry trade proxy)
  - Fed Funds rate changes affect FX desk vol

Outputs:
  data/raw/daily_pnl_{desk}.csv   ~30,000 daily P&L rows (1,889 trading days)
  data/raw/plan_budget.csv        ~1,400 monthly plan rows
  data/raw/prior_year_actuals.csv ~1,400 monthly prior-year rows
  data/raw/fx_rates.csv           ~500 monthly FX rate rows
  data/raw/calendar.csv           1,889 business-day rows
"""

import numpy as np
import pandas as pd
from pathlib import Path
from config import (
    DESKS, RAW_DIR, MACRO_DATA_PATH, FINANCIAL_TS_PATH,
    STOCK_TO_PRODUCT_MAP, EQ_SWAP_CONFIG,
    SEASONAL_FACTORS, PLAN_GROWTH_TARGET, FUNDING_CHARGE_RATE,
    FX_CURRENCIES
)


def load_macro_data() -> pd.DataFrame:
    """Load and clean macro data."""
    print(f"  Reading: {MACRO_DATA_PATH}")
    macro = pd.read_csv(MACRO_DATA_PATH, parse_dates=["Date"])
    macro = macro.sort_values("Date").reset_index(drop=True)
    macro = macro.dropna(subset=["Date"])

    # Compute daily changes for overlay signals
    macro["yield_10y_chg"] = macro["10Y Treasury Yield"].diff()
    macro["fed_funds_chg"] = macro["Fed Funds Rate"].diff()
    macro["sofr_level"] = macro["SOFR"]

    print(f"    → {len(macro)} rows | {macro['Date'].min().date()} to {macro['Date'].max().date()}")
    return macro


def load_financial_ts() -> pd.DataFrame:
    """Load financial timeseries and extract t-0 returns for each stock."""
    print(f"  Reading: {FINANCIAL_TS_PATH}")
    ts = pd.read_csv(FINANCIAL_TS_PATH)
    # We only need the t-0 (current day) standardized returns
    t0_cols = [c for c in ts.columns if c.endswith("_t-0")]
    df = ts[t0_cols + ["target"]].copy()
    df.columns = [c.replace("_t-0", "") for c in t0_cols] + ["target"]
    print(f"    → {len(df)} rows | Stocks: {list(df.columns[:-1])}")
    return df


def align_datasets(macro: pd.DataFrame, fin_ts: pd.DataFrame) -> pd.DataFrame:
    """
    Align macro dates with financial timeseries rows.
    The fin_ts has no dates, so we map business days from macro onto it.
    We take the overlapping count (min of both).
    """
    # Filter macro to business days only
    macro_bdays = macro[macro["Date"].dt.dayofweek < 5].copy()
    macro_bdays = macro_bdays.reset_index(drop=True)

    n = min(len(macro_bdays), len(fin_ts))
    print(f"  Aligning: {n} overlapping business days")

    # Take last n rows from macro (most recent) aligned with fin_ts
    macro_aligned = macro_bdays.tail(n).reset_index(drop=True)
    fin_aligned = fin_ts.head(n).reset_index(drop=True)

    combined = pd.concat([macro_aligned, fin_aligned], axis=1)
    return combined


def generate_calendar(dates: pd.Series) -> pd.DataFrame:
    """Build dim_calendar from the actual dates in the macro data."""
    cal = pd.DataFrame({"date_key": dates})
    cal["year_month"] = cal["date_key"].dt.to_period("M").astype(str)
    cal["fiscal_year"] = cal["date_key"].dt.year
    cal["fiscal_quarter"] = (
        cal["fiscal_year"].astype(str)
        + "Q"
        + cal["date_key"].dt.quarter.astype(str)
    )
    cal["business_day_of_month"] = cal.groupby("year_month").cumcount() + 1
    cal["total_business_days"] = cal.groupby("year_month")["date_key"].transform("count").astype(int)
    cal["is_month_end"] = cal["business_day_of_month"] == cal["total_business_days"]
    return cal


def generate_daily_pnl(combined: pd.DataFrame, seed: int = 42) -> dict:
    """
    Generate daily P&L for each desk/product using REAL stock returns
    from the financial timeseries, scaled to realistic dollar amounts,
    with macro overlays from the macro dataset.

    Returns dict of {desk_code: DataFrame}.
    """
    rng = np.random.default_rng(seed)
    all_rows = []

    for i, row in combined.iterrows():
        date_str = row["Date"].strftime("%Y-%m-%d")
        month_num = row["Date"].month
        seasonal = SEASONAL_FACTORS.get(month_num, 1.0)

        # Macro overlays (handle NaN from first-row diff)
        yield_chg = row.get("yield_10y_chg", 0)
        if pd.isna(yield_chg):
            yield_chg = 0
        sofr = row.get("sofr_level", 2.0)
        if pd.isna(sofr):
            sofr = 2.0
        ff_chg = row.get("fed_funds_chg", 0)
        if pd.isna(ff_chg):
            ff_chg = 0

        # ── Process each stock → products mapping ──
        for stock, mapping in STOCK_TO_PRODUCT_MAP.items():
            stock_return = row.get(stock, 0)
            if pd.isna(stock_return):
                stock_return = 0
            desk_code = mapping["desk_code"]

            for prod_code, params in mapping["products"].items():
                weight = params["weight"]
                scale = params["pnl_scale"]

                # Core P&L = standardized return × scale × weight × seasonal
                base_pnl = stock_return * scale * weight * seasonal

                # Macro overlay adjustments
                if desk_code == "FI_RATES":
                    # Yield changes amplify rates P&L
                    rate_adj = -yield_chg * scale * 0.3
                    # SOFR level boosts Repo carry
                    if prod_code == "REPO":
                        base_pnl += sofr * 10_000 * seasonal
                    base_pnl += rate_adj

                elif desk_code == "FX_COMM":
                    # Fed funds changes increase FX vol
                    vol_boost = abs(ff_chg) * scale * 0.2
                    base_pnl += vol_boost * np.sign(stock_return) if stock_return != 0 else 0

                # Add small noise so products aren't perfectly correlated
                noise = rng.normal(0, scale * weight * 0.05)
                gross_pnl = base_pnl + noise

                # Notional and trades (scaled by seasonal + absolute return magnitude)
                vol_factor = max(0.5, min(2.0, 1.0 + abs(stock_return) * 0.3))
                notional = rng.uniform(*params["notional_range"]) * seasonal * vol_factor
                num_trades = int(rng.uniform(*params["trades_range"]) * seasonal * vol_factor)

                # Net = Gross minus funding charge
                funding_charge = notional * FUNDING_CHARGE_RATE / 252
                net_pnl = gross_pnl - funding_charge

                # RWA fluctuates with market conditions
                rwa = params["rwa_base"] * rng.uniform(0.90, 1.10) * vol_factor

                all_rows.append({
                    "date_key": date_str,
                    "desk_code": desk_code,
                    "product_code": prod_code,
                    "gross_pnl_usd": round(gross_pnl, 2),
                    "net_pnl_usd": round(net_pnl, 2),
                    "notional_traded_usd": round(notional, 2),
                    "num_trades": max(1, num_trades),
                    "rwa_usd": round(rwa, 2),
                })

        # ── EQ_SWAP: blended GOOGL + MSFT signal ──
        cfg = EQ_SWAP_CONFIG
        blended_return = sum(
            (row.get(s, 0) if not pd.isna(row.get(s, 0)) else 0) * w
            for s, w in zip(cfg["blend_stocks"], cfg["blend_weights"])
        )
        gross_pnl = blended_return * cfg["pnl_scale"] * seasonal
        noise = rng.normal(0, cfg["pnl_scale"] * 0.05)
        gross_pnl += noise

        vol_factor = max(0.5, min(2.0, 1.0 + abs(blended_return) * 0.3))
        notional = rng.uniform(*cfg["notional_range"]) * seasonal * vol_factor
        num_trades = int(rng.uniform(*cfg["trades_range"]) * seasonal * vol_factor)
        funding_charge = notional * FUNDING_CHARGE_RATE / 252
        net_pnl = gross_pnl - funding_charge
        rwa = cfg["rwa_base"] * rng.uniform(0.90, 1.10) * vol_factor

        all_rows.append({
            "date_key": date_str,
            "desk_code": cfg["desk_code"],
            "product_code": cfg["product_code"],
            "gross_pnl_usd": round(gross_pnl, 2),
            "net_pnl_usd": round(net_pnl, 2),
            "notional_traded_usd": round(notional, 2),
            "num_trades": max(1, num_trades),
            "rwa_usd": round(rwa, 2),
        })

    # Split into per-desk DataFrames
    df_all = pd.DataFrame(all_rows)
    desk_frames = {}
    for desk_code in DESKS:
        desk_df = df_all[df_all["desk_code"] == desk_code].copy()
        desk_frames[desk_code] = desk_df.reset_index(drop=True)

    return desk_frames


def generate_plan_and_prior(desk_frames: dict) -> tuple:
    """
    Build plan/budget and prior-year from REAL monthly aggregates.
    Plan = actuals × (1 + growth_target) with noise, representing
    a budget that was set with a 5% YoY growth expectation.
    """
    all_daily = pd.concat(desk_frames.values(), ignore_index=True)
    all_daily["date_key"] = pd.to_datetime(all_daily["date_key"])
    all_daily["year_month"] = all_daily["date_key"].dt.to_period("M").astype(str)

    monthly = (
        all_daily
        .groupby(["year_month", "desk_code", "product_code"])
        .agg(
            net_pnl_usd=("net_pnl_usd", "sum"),
            rwa_usd=("rwa_usd", "mean"),
            notional_traded_usd=("notional_traded_usd", "sum"),
        )
        .reset_index()
    )

    # Prior year = the monthly actuals (these ARE the real numbers)
    prior_year = monthly[["year_month", "desk_code", "product_code",
                          "net_pnl_usd", "rwa_usd"]].copy()
    prior_year.rename(columns={
        "net_pnl_usd": "prior_net_pnl_usd",
        "rwa_usd": "prior_rwa_usd",
    }, inplace=True)

    # Plan = prior × (1 + growth) with ±3% noise
    rng = np.random.default_rng(123)
    plan = monthly[["year_month", "desk_code", "product_code",
                    "net_pnl_usd", "rwa_usd", "notional_traded_usd"]].copy()
    noise = rng.normal(1.0, 0.03, size=len(plan))
    plan["planned_net_pnl_usd"] = (
        plan["net_pnl_usd"] * (1 + PLAN_GROWTH_TARGET) * noise
    ).round(2)
    plan["planned_rwa_usd"] = (plan["rwa_usd"] * 1.02).round(2)
    plan["planned_notional_usd"] = (
        plan["notional_traded_usd"] * (1 + PLAN_GROWTH_TARGET) * noise
    ).round(2)
    plan = plan[["year_month", "desk_code", "product_code",
                 "planned_net_pnl_usd", "planned_rwa_usd", "planned_notional_usd"]]

    return plan, prior_year


def generate_fx_rates(dates: pd.Series, macro: pd.DataFrame) -> pd.DataFrame:
    """
    Derive FX rates from macro data: base rates adjusted by CPI / yield
    differentials to create realistic monthly FX snapshots.
    """
    rng = np.random.default_rng(99)
    months = dates.dt.to_period("M").unique()

    # Base rates with realistic drift influenced by macro
    base_rates = {
        "CAD": 1.35, "EUR": 0.92, "GBP": 0.79,
        "JPY": 149.0, "CHF": 0.88, "AUD": 1.55,
    }

    rows = []
    for period in months:
        ym_str = str(period)
        # Find the closest macro month for CPI influence
        try:
            month_end = period.to_timestamp(how="E")
            mask = macro["Date"] <= month_end
            if mask.any():
                latest = macro[mask].iloc[-1]
                cpi_factor = latest["CPI"] / 250.0
            else:
                cpi_factor = 1.0
        except Exception:
            cpi_factor = 1.0

        for ccy in FX_CURRENCIES:
            drift = (cpi_factor - 1.0) * 0.1
            rate = base_rates[ccy] * (1 + drift) * rng.uniform(0.97, 1.03)
            rows.append({
                "year_month": ym_str,
                "ccy_from": ccy,
                "ccy_to": "USD",
                "rate": round(rate, 6),
            })
            base_rates[ccy] = rate

    return pd.DataFrame(rows)


def main():
    print("=" * 65)
    print("FP&A Data Builder — Generating desk P&L from real market data")
    print("=" * 65)

    # 1. Load real datasets
    print("\n[1/6] Loading real market datasets...")
    macro = load_macro_data()
    fin_ts = load_financial_ts()

    # 2. Align
    print("\n[2/6] Aligning macro dates with financial timeseries...")
    combined = align_datasets(macro, fin_ts)
    dates = combined["Date"]
    print(f"  → Date range: {dates.min().date()} to {dates.max().date()}")
    print(f"  → {len(combined):,} trading days of real data")

    # 3. Calendar
    print("\n[3/6] Building business-day calendar from real dates...")
    calendar = generate_calendar(dates)
    cal_path = RAW_DIR / "calendar.csv"
    calendar.to_csv(cal_path, index=False)
    print(f"  → {len(calendar)} days, {calendar['year_month'].nunique()} months → {cal_path}")

    # 4. Daily P&L (driven by real returns)
    print("\n[4/6] Generating daily P&L from real stock returns + macro overlay...")
    desk_frames = generate_daily_pnl(combined)
    total_rows = 0
    for desk_code, df in desk_frames.items():
        path = RAW_DIR / f"daily_pnl_{desk_code}.csv"
        df.to_csv(path, index=False)
        total_rows += len(df)
        print(f"  → {desk_code}: {len(df):,} rows → {path}")
    print(f"  → Total daily P&L rows: {total_rows:,}")

    # 5. Plan & Prior
    print("\n[5/6] Building plan/budget and prior-year from real monthly aggregates...")
    plan, prior = generate_plan_and_prior(desk_frames)
    plan_path = RAW_DIR / "plan_budget.csv"
    prior_path = RAW_DIR / "prior_year_actuals.csv"
    plan.to_csv(plan_path, index=False)
    prior.to_csv(prior_path, index=False)
    print(f"  → Plan:  {len(plan)} rows → {plan_path}")
    print(f"  → Prior: {len(prior)} rows → {prior_path}")

    # 6. FX Rates (macro-influenced)
    print("\n[6/6] Deriving FX rates from macro data...")
    fx = generate_fx_rates(dates, macro)
    fx_path = RAW_DIR / "fx_rates.csv"
    fx.to_csv(fx_path, index=False)
    print(f"  → {len(fx)} FX rate rows → {fx_path}")

    # Summary
    n_months = calendar["year_month"].nunique()
    print(f"\n{'=' * 65}")
    print(f"  SUMMARY")
    print(f"  Data source:     Real market data (macro + financial timeseries)")
    print(f"  Date range:      {dates.min().date()} → {dates.max().date()}")
    print(f"  Trading days:    {len(combined):,}")
    print(f"  Months:          {n_months}")
    print(f"  Desks:           {len(DESKS)}")
    print(f"  Products:        16")
    print(f"  Daily P&L rows:  {total_rows:,}")
    print(f"  Plan rows:       {len(plan):,}")
    print(f"  Prior rows:      {len(prior):,}")
    print(f"  FX rate rows:    {len(fx):,}")
    print(f"  Output dir:      {RAW_DIR}")
    print(f"{'=' * 65}")


if __name__ == "__main__":
    main()
