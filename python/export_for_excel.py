"""
export_for_excel.py — Export reporting views to CSV for Excel Power Query.

This script queries the PostgreSQL reporting views and exports them
as CSV files that Excel Power Query can consume. This is useful when:
  - Direct ODBC connection to PostgreSQL is not available
  - You want to demo the project without a live database
  - You need a snapshot for the portfolio / GitHub

Exports (to data/processed/):
  - monthly_variance.csv   (full detail: desk × product × month, ~1,400 rows)
  - desk_summary.csv       (rolled up: desk × month, ~350 rows)
  - ytd_summary.csv        (YTD by desk, ~30 rows)
  - daily_pnl_detail.csv   (daily grain for Tableau, ~30,000 rows)
"""

import pandas as pd
from sqlalchemy import create_engine
from config import DB_SCHEMA, PROCESSED_DIR, get_connection_string


EXPORTS = {
    "monthly_variance": f"SELECT * FROM {DB_SCHEMA}.vw_monthly_variance ORDER BY year_month, desk_code, product_code",
    "desk_summary":     f"SELECT * FROM {DB_SCHEMA}.vw_desk_summary ORDER BY year_month, desk_code",
    "ytd_summary":      f"SELECT * FROM {DB_SCHEMA}.vw_ytd_summary ORDER BY fiscal_year, desk_code",
    "daily_pnl_detail": f"SELECT * FROM {DB_SCHEMA}.vw_daily_pnl_detail ORDER BY date_key, desk_code, product_code",
}


def main():
    print("=" * 60)
    print("FP&A Export — Extracting reporting views to CSV")
    print("=" * 60)

    engine = create_engine(get_connection_string())

    for name, query in EXPORTS.items():
        print(f"\n  Exporting {name}...")
        try:
            df = pd.read_sql(query, engine)
            out_path = PROCESSED_DIR / f"{name}.csv"
            df.to_csv(out_path, index=False)
            print(f"    → {len(df):,} rows → {out_path}")
        except Exception as e:
            print(f"    ⚠ Error: {e}")

    print(f"\n{'=' * 60}")
    print(f"✓ All exports saved to: {PROCESSED_DIR}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
