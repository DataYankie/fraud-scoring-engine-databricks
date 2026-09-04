"""Promote bronze operational tables into silver.

Usage:
    python scripts/promote_silver.py
    python scripts/promote_silver.py --dry-run

Prerequisites:
    - ``fraud.bronze.transactions`` / ``transaction_identities`` populated
    - Cluster with Spark + Delta (Databricks Runtime)
    - Package installed: %pip install -e .
"""

from __future__ import annotations

import argparse

from fraud_scoring_engine.config import silver_table
from fraud_scoring_engine.silver import promote_bronze_to_silver
from fraud_scoring_engine.spark_session import get_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Promote cleaned bronze transactions/identities into silver Delta tables."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute promote counts but do not write silver tables.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark()
    result = promote_bronze_to_silver(spark=spark, dry_run=args.dry_run)
    if args.dry_run:
        print(
            f"Would write {result.transactions_written} rows to "
            f"{silver_table('transactions')} and {result.identities_written} rows to "
            f"{silver_table('transaction_identities')}"
        )
        return
    print(
        f"Wrote {result.transactions_written} rows to {silver_table('transactions')} "
        f"and {result.identities_written} rows to {silver_table('transaction_identities')}"
    )


if __name__ == "__main__":
    main()
