"""
validate.py — Data quality checks for the FP&A pipeline.

Runs four validation checks on the raw CSV data (derived from real market
datasets) before it is loaded into PostgreSQL:
  1. Null / missing value check on critical columns
  2. Referential integrity (desk_code, product_code must exist in config)
  3. Outlier detection (daily P&L > 3σ from trailing 60-day mean)
  4. Completeness check (every desk must have data every business day)

Results are returned as a structured report and optionally logged to the
gm_fpa.dq_log table in PostgreSQL.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from config import DESKS, ALL_PRODUCTS, RAW_DIR, PROCESSED_DIR

# Build reference sets from config
VALID_DESK_CODES = set(DESKS.keys())
VALID_PRODUCT_CODES = ALL_PRODUCTS


class DQResult:
    """Container for a single data quality check result."""
    def __init__(self, check_name: str, severity: str, passed: bool,
                 details: str = "", row_count: int = 0):
        self.check_name = check_name
        self.severity = severity
        self.passed = passed
        self.details = details
        self.row_count = row_count
        self.timestamp = datetime.now()

    def __repr__(self):
        status = "PASS ✓" if self.passed else f"FAIL ✗ [{self.severity}]"
        return f"  {status}  {self.check_name}: {self.details}"


def load_all_daily_pnl() -> pd.DataFrame:
    """Load and concatenate all desk daily P&L CSVs."""
    frames = []
    for desk_code in DESKS:
        path = RAW_DIR / f"daily_pnl_{desk_code}.csv"
        if path.exists():
            df = pd.read_csv(path, parse_dates=["date_key"])
            frames.append(df)
    if not frames:
        raise FileNotFoundError("No daily P&L files found in raw directory.")
    return pd.concat(frames, ignore_index=True)


def check_nulls(df: pd.DataFrame) -> DQResult:
    """Check 1: No null values in critical columns."""
    critical_cols = ["date_key", "desk_code", "product_code",
                     "gross_pnl_usd", "net_pnl_usd"]
    null_counts = df[critical_cols].isnull().sum()
    total_nulls = null_counts.sum()

    if total_nulls == 0:
        return DQResult("null_check", "ERROR", True,
                        "No nulls found in critical columns.")
    else:
        detail = "; ".join(f"{col}={n}" for col, n in null_counts.items() if n > 0)
        return DQResult("null_check", "ERROR", False,
                        f"Nulls detected: {detail}", int(total_nulls))


def check_referential_integrity(df: pd.DataFrame) -> list:
    """Check 2: All desk_code and product_code values are valid."""
    results = []

    bad_desks = set(df["desk_code"].unique()) - VALID_DESK_CODES
    if bad_desks:
        results.append(DQResult(
            "ref_integrity_desk", "ERROR", False,
            f"Unknown desk codes: {bad_desks}",
            int(df["desk_code"].isin(bad_desks).sum())
        ))
    else:
        results.append(DQResult(
            "ref_integrity_desk", "ERROR", True,
            f"All {len(df['desk_code'].unique())} desk codes valid."
        ))

    bad_products = set(df["product_code"].unique()) - VALID_PRODUCT_CODES
    if bad_products:
        results.append(DQResult(
            "ref_integrity_product", "ERROR", False,
            f"Unknown product codes: {bad_products}",
            int(df["product_code"].isin(bad_products).sum())
        ))
    else:
        results.append(DQResult(
            "ref_integrity_product", "ERROR", True,
            f"All {len(df['product_code'].unique())} product codes valid."
        ))

    return results


def check_outliers(df: pd.DataFrame, window: int = 60, threshold: float = 3.0) -> DQResult:
    """
    Check 3: Flag daily P&L values > threshold standard deviations
    from their trailing window-day mean (per desk/product).
    """
    df_sorted = df.sort_values(["desk_code", "product_code", "date_key"])
    outlier_flags = []

    for (desk, prod), group in df_sorted.groupby(["desk_code", "product_code"]):
        pnl = group["net_pnl_usd"]
        rolling_mean = pnl.rolling(window=window, min_periods=10).mean()
        rolling_std = pnl.rolling(window=window, min_periods=10).std()

        z_scores = ((pnl - rolling_mean) / rolling_std).abs()
        outliers = z_scores > threshold
        if outliers.any():
            outlier_flags.append({
                "desk_code": desk,
                "product_code": prod,
                "count": int(outliers.sum()),
                "max_z": round(float(z_scores.max()), 2),
            })

    if not outlier_flags:
        return DQResult("outlier_check", "WARNING", True,
                        f"No outliers detected (>{threshold}σ, {window}-day window).")
    else:
        total = sum(o["count"] for o in outlier_flags)
        top3 = sorted(outlier_flags, key=lambda x: -x["max_z"])[:3]
        detail = (
            f"{total} outlier P&L days detected. "
            f"Top flagged: {', '.join(f'{o['desk_code']}/{o['product_code']} (z={o['max_z']})' for o in top3)}"
        )
        return DQResult("outlier_check", "WARNING", False, detail, total)


def check_completeness(df: pd.DataFrame, calendar_path: Path = None) -> DQResult:
    """
    Check 4: Every desk must have at least one record per business day.
    """
    if calendar_path is None:
        calendar_path = RAW_DIR / "calendar.csv"

    cal = pd.read_csv(calendar_path, parse_dates=["date_key"])
    expected_dates = set(cal["date_key"].dt.date)

    gaps = []
    for desk_code in VALID_DESK_CODES:
        desk_df = df[df["desk_code"] == desk_code]
        actual_dates = set(pd.to_datetime(desk_df["date_key"]).dt.date)
        missing = expected_dates - actual_dates
        if missing:
            gaps.append({"desk": desk_code, "missing_days": len(missing)})

    if not gaps:
        return DQResult("completeness_check", "ERROR", True,
                        f"All desks have data for all {len(expected_dates)} business days.")
    else:
        detail = "; ".join(f"{g['desk']}: {g['missing_days']} missing days" for g in gaps)
        return DQResult("completeness_check", "ERROR", False,
                        f"Missing data: {detail}",
                        sum(g["missing_days"] for g in gaps))


def run_all_checks() -> list:
    """Execute all data quality checks and return results."""
    print("=" * 60)
    print("FP&A Data Validator — Running quality checks")
    print("=" * 60)

    results = []

    # Load data
    print("\nLoading daily P&L data...")
    df = load_all_daily_pnl()
    print(f"  Loaded {len(df):,} rows across {df['desk_code'].nunique()} desks")

    # Check 1: Nulls
    print("\n[1/4] Checking for null values...")
    r = check_nulls(df)
    results.append(r)
    print(r)

    # Check 2: Referential integrity
    print("\n[2/4] Checking referential integrity...")
    ref_results = check_referential_integrity(df)
    for r in ref_results:
        results.append(r)
        print(r)

    # Check 3: Outliers
    print("\n[3/4] Checking for P&L outliers...")
    r = check_outliers(df)
    results.append(r)
    print(r)

    # Check 4: Completeness
    print("\n[4/4] Checking data completeness...")
    r = check_completeness(df)
    results.append(r)
    print(r)

    # Summary
    errors = [r for r in results if not r.passed and r.severity == "ERROR"]
    warnings = [r for r in results if not r.passed and r.severity == "WARNING"]
    print("\n" + "=" * 60)
    print(f"SUMMARY: {len(results)} checks | "
          f"{len(results) - len(errors) - len(warnings)} passed | "
          f"{len(errors)} errors | {len(warnings)} warnings")

    if errors:
        print("\n⚠  BLOCKING ERRORS — resolve before loading:")
        for e in errors:
            print(f"   • {e.check_name}: {e.details}")

    print("=" * 60)
    return results


def results_to_dataframe(results: list) -> pd.DataFrame:
    """Convert DQ results to a DataFrame for logging to PostgreSQL."""
    rows = []
    for r in results:
        rows.append({
            "run_ts": r.timestamp,
            "check_name": r.check_name,
            "severity": r.severity,
            "passed": r.passed,
            "details": r.details,
            "row_count": r.row_count,
        })
    return pd.DataFrame(rows)


if __name__ == "__main__":
    results = run_all_checks()

    # Save results to CSV for reference
    df_results = results_to_dataframe(results)
    out_path = PROCESSED_DIR / "dq_results.csv"
    df_results.to_csv(out_path, index=False)
    print(f"\nResults saved to: {out_path}")
