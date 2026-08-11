"""Download IEEE-CIS Fraud Detection competition data into the Volume raw dir.

Usage:
    python scripts/download_ieee_fraud_data.py

Requires Kaggle credentials via KAGGLE_API_TOKEN or ~/.kaggle/access_token.
You must join and accept the competition rules at:
https://www.kaggle.com/competitions/ieee-fraud-detection

Default output: /Volumes/fraud/bronze/data/raw/
Override with FRAUD_CATALOG / FRAUD_SCHEMA / FRAUD_VOLUME or --data-dir.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import kagglehub

from fraud_scoring_engine.ingest.paths import ieee_data_paths

COMPETITION = "ieee-fraud-detection"
EXPECTED_FILES = (
    "train_transaction.csv",
    "train_identity.csv",
    "test_transaction.csv",
    "test_identity.csv",
    "sample_submission.csv",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download IEEE-CIS competition CSVs into the Volume raw directory.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Output directory (default: /Volumes/fraud/bronze/data/raw).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = ieee_data_paths(args.data_dir).data_dir
    data_dir.mkdir(parents=True, exist_ok=True)

    path = kagglehub.competition_download(
        COMPETITION,
        output_dir=str(data_dir),
        force_download=True,
    )
    print(f"Downloaded to: {path}")

    missing = [name for name in EXPECTED_FILES if not (data_dir / name).exists()]
    if missing:
        raise FileNotFoundError(f"Expected files missing in {data_dir}: {', '.join(missing)}")


if __name__ == "__main__":
    main()
