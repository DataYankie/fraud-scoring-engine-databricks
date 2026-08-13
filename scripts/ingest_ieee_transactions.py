"""Ingest IEEE-CIS train transactions into Unity Catalog Delta tables.

Usage:
    python scripts/ingest_ieee_transactions.py --limit 10000
    python scripts/ingest_ieee_transactions.py
    python scripts/ingest_ieee_transactions.py --limit 100 --dry-run
    python scripts/ingest_ieee_transactions.py --skip-tables --limit 10000
    python scripts/ingest_ieee_transactions.py --skip-features

Merges operational columns into ``fraud.bronze.transactions`` /
``fraud.bronze.transaction_identities`` and overwrites
``fraud.bronze.train_features`` with the remaining CSV columns.

Prerequisites:
    - Raw CSVs in /Volumes/fraud/bronze/data/raw/ (see download_ieee_fraud_data.py)
    - Cluster with Spark + Delta (Databricks Runtime)
    - Package installed: %pip install -e .
"""

from __future__ import annotations

import subprocess
import sys
import argparse
from pathlib import Path

# Install fraud_scoring_engine package for this script execution
subprocess.check_call([
    sys.executable, "-m", "pip", "install", "-q",
    "/Workspace/Users/yannickkh@outlook.com/fraud-scoring-engine"
])
from fraud_scoring_engine.ingest.loader import ingest_train_transactions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest IEEE-CIS train data into bronze Delta tables "
            "(transactions, identities, train_features)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N transaction rows (for dev iteration).",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Path to raw CSVs (default: /Volumes/fraud/bronze/data/raw).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and transform only; do not write Delta tables.",
    )
    parser.add_argument(
        "--skip-tables",
        action="store_true",
        help="Write train_features only; skip transactions/identities MERGE.",
    )
    parser.add_argument(
        "--skip-features",
        action="store_true",
        help="MERGE operational tables only; skip train_features overwrite.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ingest_train_transactions(
        data_dir=args.data_dir,
        limit=args.limit,
        dry_run=args.dry_run,
        skip_tables=args.skip_tables,
        skip_features=args.skip_features,
    )


if __name__ == "__main__":
    main()
