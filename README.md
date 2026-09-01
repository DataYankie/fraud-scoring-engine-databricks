# Fraud Scoring Engine
> 🚧 **Status:** Under active development - bronze ingest on Databricks Volumes + Delta; training experiments and scoring service coming next.

End-to-end system for scoring payment transactions for fraud risk, built around the [IEEE-CIS Fraud Detection](https://www.kaggle.com/c/ieee-fraud-detection) dataset.

## Repository structure

Each folder has a README explaining its purpose. Start here to find your way around:

| Folder | Description |
|--------|-------------|
| [`.github/`](.github/README.md) | GitHub automation and CI configuration |
| [`docs/`](docs/README.md) | Architecture diagrams and design artifacts |
| [`notebooks/`](notebooks/README.md) | EDA, ingest validation, and XGBoost training notebooks |
| [`scripts/`](scripts/README.md) | Operational CLI commands for the bronze pipeline |
| [`src/`](src/README.md) | Python package source (`fraud_scoring_engine`) |
| [`tests/`](tests/README.md) | pytest suite for the library |

The `src/fraud_scoring_engine/` package is further split into [delta](src/fraud_scoring_engine/delta/README.md), [ingest](src/fraud_scoring_engine/ingest/README.md), [preprocessing](src/fraud_scoring_engine/preprocessing/README.md), and [training](src/fraud_scoring_engine/training/README.md).

## Current progress
- [x] Data download to Unity Catalog Volume
- [x] Bronze Delta tables (transactions, identities, train_features)
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
2. Create Volume `/Volumes/fraud/bronze/data` (catalog `fraud`, schema `bronze`, volume `data`) if it does not exist.
3. On a cluster notebook or job:

```python
%pip install -e /Workspace/Repos/<path-to-this-repo>
```

4. Run the data pipeline (see [scripts/README.md](scripts/README.md)).

### Defaults (override with env)
| Setting | Env var | Default |
|---------|---------|---------|
| Catalog | `FRAUD_CATALOG` | `fraud` |
| Schema | `FRAUD_SCHEMA` | `bronze` |
| Volume | `FRAUD_VOLUME` | `data` |

Raw CSVs: `/Volumes/fraud/bronze/data/raw/`  
Tables: `fraud.bronze.transactions`, `fraud.bronze.transaction_identities`, `fraud.bronze.train_features`, `fraud.bronze.behavioral_features`, `fraud.bronze.fraud_alerts`
