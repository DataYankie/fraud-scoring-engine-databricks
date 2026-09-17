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
| [`resources/jobs/`](resources/jobs/README.md) | DAB job definitions for pipeline automation |
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

## Job deployment (Declarative Automation Bundles)

This project uses **Declarative Automation Bundles (DABs)** to manage jobs declaratively. All job definitions are in `resources/jobs/*.yml`.

### Prerequisites
* Databricks CLI installed: `pip install databricks-cli`
* Authenticated to your workspace: `databricks configure`
* Catalogs created: `fraud_dev` (for dev) and `fraud` (for prod)

### Deploy jobs

```bash
cd /<path-to-this-repo>/fraud-scoring-engine

# Deploy to dev environment (uses fraud_dev catalog)
databricks bundle deploy

# Deploy to prod environment (uses fraud catalog)
databricks bundle deploy -t prod
```

This creates/updates three jobs:
* **Ingesting** - Ingest raw data to bronze, promote to silver
* **Generete behavioral features** - Generate features from silver to gold
* **Run integration tests** - Run pytest integration tests

### Run a deployed job

```bash
# Run in dev
databricks bundle run Ingesting

# Run in prod
databricks bundle run Ingesting -t prod
```

### Job recovery

If jobs are accidentally deleted, simply re-run `databricks bundle deploy` to recreate them from the YAML definitions.

### Defaults (override with env)
| Setting | Env var | Default |
|---------|---------|---------|
| Catalog | `FRAUD_CATALOG` | `fraud_dev` |
| Bronze schema | `FRAUD_BRONZE_SCHEMA` | `bronze` |
| Silver schema | `FRAUD_SILVER_SCHEMA` | `silver` |
| Gold schema | `FRAUD_GOLD_SCHEMA` | `gold` |
| Volume | `FRAUD_VOLUME` | `data` |

### Medallion architecture paths

**Development (default):**
- Raw CSVs: `/Volumes/fraud_dev/bronze/data/raw/`
- Bronze: `fraud_dev.bronze.transactions`, `fraud_dev.bronze.transaction_identities`, `fraud_dev.bronze.train_features`
- Silver: `fraud_dev.silver.transactions`, `fraud_dev.silver.transaction_identities`
- Gold: `fraud_dev.gold.behavioral_features`, `fraud_dev.gold.fraud_alerts`

**Production:**
- Raw CSVs: `/Volumes/fraud/bronze/data/raw/`
- Bronze: `fraud.bronze.transactions`, `fraud.bronze.transaction_identities`, `fraud.bronze.train_features`
- Silver: `fraud.silver.transactions`, `fraud.silver.transaction_identities`
- Gold: `fraud.gold.behavioral_features`, `fraud.gold.fraud_alerts`
