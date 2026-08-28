# training

**Purpose:** Training dataset assembly from Delta tables, time-based train/val/test splits, and MLflow logging utilities.

## Contents

- `dataset.py` - `build_training_frame` and static feature loaders from bronze Delta tables
- `split.py` - time-based `time_split` with configurable ratios
- `mlflow_utils.py` - `setup_mlflow` for experiment tracking configuration

## Related

- [fraud_scoring_engine/README.md](../README.md)
- [preprocessing/README.md](../preprocessing/README.md) - feature preprocessing after frame assembly
- [notebooks/README.md](../../../notebooks/README.md) - XGBoost training experiments
- [scripts/README.md](../../../scripts/README.md) - `generate_training_data.py` for behavioral features
