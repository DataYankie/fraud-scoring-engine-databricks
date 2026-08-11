# Scripts

Operational commands for the fraud-scoring-engine bronze pipeline on Databricks.

## Prerequisites
- Databricks cluster (Spark + Delta)
- Package installed: `%pip install -e /Workspace/Repos/<path-to-this-repo>`
- Unity Catalog Volume at `/Volumes/fraud/bronze/data` (or override via `FRAUD_*` env vars)
- Kaggle credentials for the download step

## Workflow

1. Download IEEE data to the Volume
2. Ensure bronze Delta tables
3. Ingest (dev → full)
4. Generate behavioral features

### 1. Download data
```bash
python scripts/download_ieee_fraud_data.py
```

Writes to `/Volumes/fraud/bronze/data/raw/` by default. Override with `--data-dir`.

### 2. Ensure schema / tables
```python
from fraud_scoring_engine.spark_session import get_spark
from fraud_scoring_engine.delta import ensure_bronze_tables

ensure_bronze_tables(get_spark())
```

Ingest also calls `ensure_bronze_tables` unless disabled.

### 3. Ingest transactions
Operational columns MERGE into Delta. Remaining CSV columns overwrite `fraud.bronze.train_features`.

#### Dev pass (10k rows)
```bash
python scripts/ingest_ieee_transactions.py --limit 10000
```

#### Dry run
```bash
python scripts/ingest_ieee_transactions.py --limit 100 --dry-run
```

#### Features only
```bash
python scripts/ingest_ieee_transactions.py --skip-tables --limit 10000
```

#### Tables only
```bash
python scripts/ingest_ieee_transactions.py --skip-features
```

#### Full train load
```bash
python scripts/ingest_ieee_transactions.py
```

### 4. Generate behavioral training features
Computes rolling velocity, spend, and amount-ratio features from `fraud.bronze.transactions` and overwrites `fraud.bronze.behavioral_features`.

```bash
python scripts/generate_training_data.py --limit 10000
```

#### Training features in Python
```python
features = spark.table("fraud.bronze.train_features")
# Join to transactions on features.TransactionID == transactions.transaction_id
```
