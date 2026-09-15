# Fraud Scoring Engine
> 🚧 **Status:** Under active development — core medallion ingest on Databricks Volumes + Delta is in place; training pipeline and scoring service coming next.

End-to-end system for scoring payment transactions for fraud risk, built around the [IEEE-CIS Fraud Detection](https://www.kaggle.com/c/ieee-fraud-detection) dataset.

## Repository structure

Each folder has a README explaining its purpose. Start here to find your way around:

| Folder | Description |
|--------|-------------|
| [`.github/`](.github/workflows/README.md) | GitHub automation and CI configuration |
| [`docs/`](docs/README.md) | Architecture diagrams and design artifacts |
| [`notebooks/`](notebooks/README.md) | EDA, ingest validation, and XGBoost training notebooks |
| [`scripts/`](scripts/README.md) | Operational CLI commands for the bronze pipeline |
| [`src/`](src/README.md) | Python package source (`fraud_scoring_engine`) |
| [`tests/`](tests/README.md) | pytest suite for the library |

The `src/fraud_scoring_engine/` package is further split into [delta](src/fraud_scoring_engine/delta/README.md), [ingest](src/fraud_scoring_engine/ingest/README.md), [silver](src/fraud_scoring_engine/silver/README.md), [preprocessing](src/fraud_scoring_engine/preprocessing/README.md), and [training](src/fraud_scoring_engine/training/README.md).

## Current progress
- [x] Data download to Unity Catalog Volume
- [x] Medallion Delta tables (bronze / silver / gold)
- [x] Feature scaffolding + tests/CI
- [x] EDA
- [x] XGBoost notebook (preprocess + time split + Optuna + MLflow)
- [ ] Training pipeline
- [ ] Scoring API
- [ ] Evaluation / monitoring

## Stack
Python, Databricks (Spark + Delta + Unity Catalog Volumes), scikit-learn/XGBoost, GitHub Actions

## Databricks setup

1. Clone this repo into a **Databricks Git folder (Repos)**.
2. Create Volume `/Volumes/fraud/bronze/data` (catalog `fraud`, schema `bronze`, volume `data`) if it does not exist. Also create schemas `fraud.silver` and `fraud.gold` (or rely on `ensure_medallion_tables`).
3. On a cluster notebook or job:

```python
%pip install -e /Workspace/Repos/<path-to-this-repo>
```

4. Run the data pipeline (see [scripts/README.md](scripts/README.md)).

### Defaults (override with env)
| Setting | Env var | Default |
|---------|---------|---------|
| Catalog | `FRAUD_CATALOG` | `fraud` |
| Bronze schema | `FRAUD_BRONZE_SCHEMA` (or legacy `FRAUD_SCHEMA`) | `bronze` |
| Silver schema | `FRAUD_SILVER_SCHEMA` | `silver` |
| Gold schema | `FRAUD_GOLD_SCHEMA` | `gold` |
| Volume | `FRAUD_VOLUME` | `data` |

Raw CSVs: `/Volumes/fraud/bronze/data/raw/`  
Bronze: `fraud.bronze.transactions`, `fraud.bronze.transaction_identities`, `fraud.bronze.train_features`  
Silver: `fraud.silver.transactions`, `fraud.silver.transaction_identities`  
Gold: `fraud.gold.behavioral_features`, `fraud.gold.fraud_alerts`

## Data pipeline
See [scripts/README.md](scripts/README.md) for downloading IEEE-CIS data and ingesting into Databricks Volumes / Delta tables.
