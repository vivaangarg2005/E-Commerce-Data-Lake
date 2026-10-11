"""Unit tests for incremental processing state manager."""

from pathlib import Path

import pytest

from ecommerce_data_lake.incremental import IncrementalStateManager


@pytest.mark.unit
def test_incremental_state_lifecycle(tmp_path: Path):
    mgr = IncrementalStateManager(str(tmp_path))

    # Initial state
    assert not mgr.is_batch_committed("batch_100")

    # Begin batch
    mgr.begin_batch("batch_100")
    assert not mgr.is_batch_committed("batch_100")
    assert mgr.manifest.batches["batch_100"]["status"] == "IN_PROGRESS"

    # Commit batch
    mgr.commit_batch(
        batch_id="batch_100",
        records_read=500,
        records_silver=490,
        records_quarantine=10,
        batch_watermark="2025-01-01T12:00:00",
    )
    assert mgr.is_batch_committed("batch_100")
    assert mgr.manifest.global_watermark == "2025-01-01T12:00:00"

    # Reload manifest from disk to ensure persistence
    reloaded = IncrementalStateManager(str(tmp_path))
    assert reloaded.is_batch_committed("batch_100")
    assert reloaded.manifest.batches["batch_100"]["records_read"] == 500


@pytest.mark.unit
def test_incremental_failure_does_not_advance_watermark(tmp_path: Path):
    mgr = IncrementalStateManager(str(tmp_path))
    mgr.commit_batch("batch_1", 100, 100, 0, "2025-01-01T00:00:00")

    # Start and fail batch 2
    mgr.begin_batch("batch_2")
    mgr.fail_batch("batch_2", "Simulated disk failure")

    assert not mgr.is_batch_committed("batch_2")
    assert mgr.manifest.batches["batch_2"]["status"] == "FAILED"
    # Watermark remains at batch_1
    assert mgr.manifest.global_watermark == "2025-01-01T00:00:00"
