"""
run_pipeline.py — Orchestrator for the FP&A Reporting Automation pipeline.

Runs the full end-to-end pipeline:
  1. simulate_data  → Build desk P&L from real market data (Apr 2018 – Jun 2025)
  2. validate       → Run data quality checks on raw CSVs
  3. load_to_pg     → Transform and load into PostgreSQL

Can also be run in partial mode:
  python run_pipeline.py --step simulate
  python run_pipeline.py --step validate
  python run_pipeline.py --step load
  python run_pipeline.py --step all        (default)

If validation finds blocking errors (severity=ERROR, passed=False),
the load step is skipped unless --force is passed.
"""

import argparse
import sys
import time
from datetime import datetime


def run_simulate():
    """Step 1: Generate simulated data."""
    print("\n" + "█" * 60)
    print("  STEP 1 / 3 — SIMULATE DATA")
    print("█" * 60)
    from simulate_data import main as simulate_main
    simulate_main()


def run_validate() -> bool:
    """Step 2: Validate data quality. Returns True if no blocking errors."""
    print("\n" + "█" * 60)
    print("  STEP 2 / 3 — VALIDATE DATA")
    print("█" * 60)
    from validate import run_all_checks
    results = run_all_checks()

    blocking_errors = [r for r in results if not r.passed and r.severity == "ERROR"]
    return len(blocking_errors) == 0


def run_load():
    """Step 3: Load data into PostgreSQL."""
    print("\n" + "█" * 60)
    print("  STEP 3 / 3 — LOAD INTO POSTGRESQL")
    print("█" * 60)
    from load_to_pg import main as load_main
    load_main()


def main():
    parser = argparse.ArgumentParser(
        description="FP&A Reporting Automation — Pipeline Orchestrator"
    )
    parser.add_argument(
        "--step",
        choices=["simulate", "validate", "load", "all"],
        default="all",
        help="Which pipeline step to run (default: all)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force load even if validation has blocking errors"
    )
    args = parser.parse_args()

    start_time = time.time()
    print("=" * 60)
    print("FP&A REPORTING AUTOMATION — PIPELINE")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Step:    {args.step}")
    print("=" * 60)

    if args.step in ("simulate", "all"):
        run_simulate()

    if args.step in ("validate", "all"):
        passed = run_validate()
        if not passed and not args.force:
            print("\n⚠  Blocking validation errors found. Load step skipped.")
            print("   Rerun with --force to load anyway, or fix the data first.")
            if args.step == "all":
                sys.exit(1)

    if args.step in ("load", "all"):
        run_load()

    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print(f"✓ Pipeline finished in {elapsed:.1f} seconds")
    print(f"  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
