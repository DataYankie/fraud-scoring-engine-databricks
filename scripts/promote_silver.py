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
import os

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
    parser.add_argument(
        "--schema-silver",
        type=str,
        default=None,
        help="Silver schema name (overrides FRAUD_SILVER_SCHEMA env var).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
    # Set environment variables from CLI arguments if provided
    if args.catalog:
        os.environ["FRAUD_CATALOG"] = args.catalog
    if args.schema_bronze:
        os.environ["FRAUD_BRONZE_SCHEMA"] = args.schema_bronze
    if args.schema_silver:
        os.environ["FRAUD_SILVER_SCHEMA"] = args.schema_silver
    
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
