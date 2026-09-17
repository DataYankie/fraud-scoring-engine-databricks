"""Ingest IEEE-CIS train transactions into Unity Catalog bronze Delta tables.

Usage:
    python scripts/ingest_ieee_transactions.py --limit 10000
    python scripts/ingest_ieee_transactions.py
    python scripts/ingest_ieee_transactions.py --limit 100 --dry-run
    python scripts/ingest_ieee_transactions.py --skip-tables --limit 10000
    python scripts/ingest_ieee_transactions.py --skip-features
    python scripts/ingest_ieee_transactions.py --skip-silver

Merges operational columns into ``fraud.bronze.transactions`` /
``fraud.bronze.transaction_identities`` and MERGE
``fraud.bronze.train_features`` with the remaining CSV columns. By default also
promotes cleaned operational tables to silver.

Prerequisites:
    - Raw CSVs in /Volumes/fraud/bronze/data/raw/ (see download_ieee_fraud_data.py)
    - Cluster with Spark + Delta (Databricks Runtime)
    - Package installed: %pip install -e .
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from fraud_scoring_engine.ingest.loader import ingest_train_transactions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest IEEE-CIS train data into bronze Delta tables "
            "(transactions, identities, train_features) and optionally promote silver."
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
        help="MERGE operational tables only; skip train_features MERGE.",
    )
    parser.add_argument(
        "--skip-silver",
        action="store_true",
        help="Skip bronze → silver promotion after operational MERGE.",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default=None,
        help="Unity Catalog name (overrides FRAUD_CATALOG env var).",
    )
    parser.add_argument(
        "--schema-bronze",
        type=str,
        default=None,
        help="Bronze schema name (overrides FRAUD_BRONZE_SCHEMA env var).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
    # Set environment variables from CLI arguments if provided
    if args.catalog:
        os.environ["FRAUD_CATALOG"] = args.catalog
    if args.schema_bronze:
        os.environ["FRAUD_BRONZE_SCHEMA"] = args.schema_bronze
    
    ingest_train_transactions(
        data_dir=args.data_dir,
        limit=args.limit,
        dry_run=args.dry_run,
        skip_tables=args.skip_tables,
        skip_features=args.skip_features,
        promote_silver=not args.skip_silver,
    )


if __name__ == "__main__":
    main()
