# Jobs

Declarative Automation Bundle (DAB) job definitions for the fraud-scoring-engine pipeline.

## Overview

This folder contains YAML definitions for all Databricks jobs. Jobs are deployed using the Databricks CLI and automatically use the correct catalog and schemas based on the target environment (dev or prod).

## Job definitions

### `ingestion_job.yml` - Ingesting
Two-task workflow that ingests raw IEEE-CIS fraud data and promotes it through the medallion architecture:

1. **Run_ingesting_to_bronze** - Reads raw CSVs from Unity Catalog Volume and merges into bronze Delta tables
2. **ingest_to_silver** - Cleans and promotes bronze operational tables to silver

**Schedule:** Manual trigger  
**Parameters:** `--limit 10000` for dev iteration  
**Scripts:** `scripts/ingest_ieee_transactions.py`, `scripts/promote_silver.py`

### `generete_behavioral_features_job.yml` - Generete behavioral features
Scheduled job that materializes behavioral features from silver to gold:

* Computes rolling velocity, spend, and amount-ratio features
* Reads from `{catalog}.silver.transactions`
* Writes to `{catalog}.gold.behavioral_features`

**Schedule:** Daily at 00:00:07 CET (paused by default)  
**Parameters:** `--limit 10000` for dev iteration  
**Script:** `scripts/generate_behavioral_features.py`

### `integration_testing.yml` - Run integration tests
Scheduled pytest integration test suite:

* Runs all tests marked with `@pytest.mark.integration`
* Validates end-to-end pipeline correctness

**Schedule:** Daily at 00:00:55 CET (paused by default)  
**Script:** `scripts/run_integration_tests.py`

## Deployment

### Prerequisites
* Databricks CLI installed: `pip install databricks-cli`
* Authenticated to workspace: `databricks configure`
* Catalogs exist: `fraud_dev` (dev) and `fraud` (prod)

### Deploy jobs

From the project root:

```bash
# Deploy to dev (uses fraud_dev catalog)
databricks bundle deploy

# Deploy to prod (uses fraud catalog)
databricks bundle deploy -t prod
```

### Run a job manually

```bash
# Run in dev
databricks bundle run Ingesting
databricks bundle run "Generete behavioral features"
databricks bundle run "Run integration tests"

# Run in prod
databricks bundle run Ingesting -t prod
```

## Environment variables passed to jobs

All jobs automatically receive catalog and schema information via command-line arguments that override the default environment variables:

* `--catalog` → Sets `FRAUD_CATALOG` (default: `fraud_dev`)
* `--schema-bronze` → Sets `FRAUD_BRONZE_SCHEMA` (default: `bronze`)
* `--schema-silver` → Sets `FRAUD_SILVER_SCHEMA` (default: `silver`)
* `--schema-gold` → Sets `FRAUD_GOLD_SCHEMA` (default: `gold`)

**Dev deployment:**
```
ffraud_dev.bronze → fraud_dev.silver → fraud_dev.gold
```

**Prod deployment:**
```
fraud.bronze → fraud.silver → fraud.gold
```

## Job recovery

If jobs are accidentally deleted from the workspace, recreate them by redeploying:

```bash
databricks bundle deploy
```

The YAML definitions are the source of truth. All job configuration (tasks, schedules, compute, parameters) is version-controlled here.

## Modifying jobs

1. Edit the YAML file in this folder
2. Validate: `databricks bundle validate`
3. Deploy: `databricks bundle deploy` (or `-t prod`)
4. Jobs are updated in-place; run history is preserved

## Adding new jobs

1. Create a new `{job_name}.job.yml` file in this folder
2. Follow the existing structure (see `ingestion_job.yml` as a template)
3. Use `${var.catalog}`, `${var.schema_bronze}`, etc. for environment-aware parameters
4. Deploy with `databricks bundle deploy`

The bundle automatically discovers all `*.yml` files via the `include: resources/**/*.yml` pattern in the root `databricks.yml`.
