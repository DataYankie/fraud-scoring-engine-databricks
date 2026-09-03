"""Generate gold behavioral training features from silver Delta transactions.

Usage:
    python scripts/generate_training_data.py --limit 10000
    python scripts/generate_training_data.py --limit 10000 --dry-run

Prerequisites:
    - ``fraud.silver.transactions`` populated (see ingest / promote_silver)
    - Cluster with Spark + Delta (Databricks Runtime)
    - Package installed: %pip install -e .
"""

from __future__ import annotations

import argparse

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
        help="Maximum number of transactions to export (default: 10000).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute features but do not write the Delta table.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
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
