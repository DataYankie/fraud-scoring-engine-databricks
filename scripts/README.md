# Scripts

Operational commands for the fraud-scoring-engine medallion pipeline on Databricks.

## Prerequisites
- Databricks cluster (Spark + Delta)
- Package installed: `%pip install -e /Workspace/Repos/<path-to-this-repo>`
- Unity Catalog Volume at `/Volumes/fraud/bronze/data` (landing zone)
- Schemas `fraud.bronze`, `fraud.silver`, and `fraud.gold` (created by `ensure_medallion_tables`)
- Kaggle credentials for the download step

## Workflow

1. Download IEEE data to the bronze Volume
2. Ensure medallion Delta tables
3. Ingest to bronze (+ promote silver by default)
4. Materialize gold behavioral features (optional / scheduled)
5. Train XGBoost (notebook)

### 1. Download data
```bash
python scripts/download_ieee_fraud_data.py
```

Writes to `/Volumes/fraud/bronze/data/raw/` by default. Override with `--data-dir`.

### 2. Ensure schemas / tables
```python
from fraud_scoring_engine.spark_session import get_spark
from fraud_scoring_engine.delta import ensure_medallion_tables

ensure_medallion_tables(get_spark())
```

Ingest also calls `ensure_medallion_tables` (or bronze-only) unless disabled.

### 3. Ingest transactions (bronze → silver)
Operational columns MERGE into bronze Delta. Remaining CSV columns MERGE into
`fraud.bronze.train_features`. By default, cleaned operational tables are
promoted to silver.

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

#### Skip silver promote
```bash
python scripts/ingest_ieee_transactions.py --skip-silver
```

#### Promote silver separately
```bash
python scripts/promote_silver.py
```

#### Full train load
```bash
python scripts/ingest_ieee_transactions.py
```

### 4. Materialize gold behavioral features
Computes rolling velocity, spend, and amount-ratio features from
`fraud.silver.transactions` and overwrites `fraud.gold.behavioral_features`.
This is an optional gold snapshot for inspection or scheduled (e.g. nightly)
jobs. Training does **not** read this table; it recomputes the same features
on the fly from silver.

#### Dev pass (10k rows)
```bash
python scripts/generate_behavioral_features.py --limit 10000
```

#### Full export (all silver transactions)
```bash
python scripts/generate_behavioral_features.py --limit -1
```

#### Dry run
```bash
python scripts/generate_behavioral_features.py --limit 10000 --dry-run
```

### 5. Train XGBoost (notebook)
Open `notebooks/xgboost.ipynb` on a Databricks cluster. It builds a training
frame from medallion Delta tables (bronze static features + silver entities +
on-the-fly behavioral features), time-splits, preprocesses, and logs baseline +
Optuna runs to workspace MLflow.

Override tracking/artifacts with `MLFLOW_TRACKING_URI` and `MLFLOW_ARTIFACT_ROOT`
if needed. On Databricks Runtime, artifacts default to
`/Volumes/fraud/bronze/data/mlartifacts`.

#### Training features in Python
```python
from fraud_scoring_engine.training import build_training_frame

df = build_training_frame(spark, limit=10_000)
```
