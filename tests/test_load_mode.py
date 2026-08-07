import os

import pytest


os.environ.setdefault("SOCRATA_APP_TOKEN", "test-token")
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_DB", "test")
os.environ.setdefault("POSTGRES_USER", "test")
os.environ.setdefault("POSTGRES_PASSWORD", "test")

from src.ingestion import extract_incremental
from src.ingestion.extract_incremental import choose_load_mode


def test_no_changes():
    result = choose_load_mode(0, 100)

    assert result == "NO_CHANGES"


def test_full_snapshot():
    result = choose_load_mode(100, 100)

    assert result == "FULL_SNAPSHOT"


def test_incremental_load():
    result = choose_load_mode(10, 100)

    assert result == "INCREMENTAL"


def test_large_change_requires_review():
    result = choose_load_mode(80, 100)

    assert result == "REVIEW_REQUIRED"


def test_negative_change_count_is_rejected():
    with pytest.raises(ValueError):
        choose_load_mode(-1, 100)


def test_change_count_cannot_exceed_source():
    with pytest.raises(ValueError):
        choose_load_mode(101, 100)


def test_no_change_run_uses_no_changes_run_type(
    monkeypatch,
):
    recorded_values = {}

    def fake_record_non_extracting_run(**values):
        recorded_values.update(values)

    monkeypatch.setattr(
        extract_incremental,
        "record_non_extracting_run",
        fake_record_non_extracting_run,
    )

    extract_incremental.record_no_change_run()

    assert recorded_values == {
        "run_type": "NO_CHANGES",
        "pipeline_status": "SUCCESS",
        "batch_status": "SUCCESS",
    }