"""Tests for Volume path defaults."""

from pathlib import Path

from fraud_scoring_engine.ingest.paths import ieee_data_paths, repo_root


def test_ieee_data_paths_defaults_to_volume() -> None:
    paths = ieee_data_paths()
    assert paths.data_dir == Path("/Volumes/fraud/bronze/data/raw")
    assert paths.processed_dir == Path("/Volumes/fraud/bronze/data/processed")
    assert paths.train_transaction.name == "train_transaction.csv"
    assert paths.train_identity.name == "train_identity.csv"


def test_ieee_data_paths_overrides(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    processed = tmp_path / "processed"
    paths = ieee_data_paths(raw, processed_dir=processed)
    assert paths.data_dir == raw
    assert paths.processed_dir == processed
    assert paths.train_transaction == raw / "train_transaction.csv"


def test_repo_root_contains_src() -> None:
    root = repo_root()
    assert (root / "src" / "fraud_scoring_engine").is_dir()
