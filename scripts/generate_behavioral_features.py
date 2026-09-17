"""Materialize gold behavioral features from silver Delta transactions.

Computes rolling velocity, spend, and amount-ratio features for transactions
in ``fraud.silver.transactions`` and overwrites ``fraud.gold.behavioral_features``.
Intended for scheduled (e.g. nightly) runs; training recomputes the same
features on the fly and does not read this table.

Usage:
    python scripts/generate_behavioral_features.py --limit 10000
    python scripts/generate_behavioral_features.py --limit -1
    python scripts/generate_behavioral_features.py --limit 10000 --dry-run

Prerequisites:
    - ``fraud.silver.transactions`` populated (see ingest / promote_silver)
    - Cluster with Spark + Delta (Databricks Runtime)
    - Package installed: %pip install -e .
"""

from __future__ import annotations

import argparse
import os

from fraud_scoring_engine.features import (
    compute_transaction_features_dataframe,
    write_behavioral_features,
)
from fraud_scoring_engine.config import gold_table
from fraud_scoring_engine.spark_session import get_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute rolling behavioral features for transactions and write "
            "fraud.gold.behavioral_features."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10_000,
        help=(
            "Maximum number of transactions to export in chronological order "
            "(default: 10000). Use a negative value to export all rows."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute features but do not write the Delta table.",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default=None,
        help="Unity Catalog name (overrides FRAUD_CATALOG env var).",
    )
    parser.add_argument(
        "--schema-silver",
        type=str,
        default=None,
        help="Silver schema name (overrides FRAUD_SILVER_SCHEMA env var).",
    )
    parser.add_argument(
        "--schema-gold",
        type=str,
        default=None,
        help="Gold schema name (overrides FRAUD_GOLD_SCHEMA env var).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
    # Set environment variables from CLI arguments if provided
    if args.catalog:
        os.environ["FRAUD_CATALOG"] = args.catalog
    if args.schema_silver:
        os.environ["FRAUD_SILVER_SCHEMA"] = args.schema_silver
    if args.schema_gold:
        os.environ["FRAUD_GOLD_SCHEMA"] = args.schema_gold
    
    spark = get_spark()
    limit = None if args.limit < 0 else args.limit

    if args.dry_run:
        dataframe = compute_transaction_features_dataframe(spark, limit=limit)
        print(f"Dry run: computed behavioral features for {len(dataframe)} transactions.")
        print(f"Would write to {gold_table('behavioral_features')}")
        return

    count = write_behavioral_features(spark, limit=limit)
    print(f"Wrote {count} rows to {gold_table('behavioral_features')}")


if __name__ == "__main__":
    main()
