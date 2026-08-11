"""Path resolution for IEEE-CIS raw data on Databricks Volumes."""

from dataclasses import dataclass
from pathlib import Path

from fraud_scoring_engine.config import volume_root


@dataclass(frozen=True)
class IeeeDataPaths:
    """Paths to train CSV files under the Volume ``raw/`` directory."""

    data_dir: Path
    train_transaction: Path
    train_identity: Path
    processed_dir: Path


def repo_root() -> Path:
    """Return the repository root (parent of ``src/``)."""
    return Path(__file__).resolve().parents[3]


def ieee_data_paths(
    data_dir: Path | None = None,
    *,
    processed_dir: Path | None = None,
) -> IeeeDataPaths:
    """Build paths to IEEE train CSVs under the Volume (or overrides).

    Args:
        data_dir: Optional override for the raw data directory.
            Defaults to ``{volume_root}/raw``.
        processed_dir: Optional override for ``{volume_root}/processed``.

    Returns:
        Resolved paths for train transaction and identity CSV files.
    """
    root = Path(data_dir) if data_dir is not None else Path(volume_root()) / "raw"
    processed = (
        Path(processed_dir)
        if processed_dir is not None
        else Path(volume_root()) / "processed"
    )
    return IeeeDataPaths(
        data_dir=root,
        train_transaction=root / "train_transaction.csv",
        train_identity=root / "train_identity.csv",
        processed_dir=processed,
    )
