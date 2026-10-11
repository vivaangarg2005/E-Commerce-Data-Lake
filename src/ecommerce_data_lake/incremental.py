"""Incremental processing coordinator: manifest tracking, watermarks, and idempotency guarantees."""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from ecommerce_data_lake.logging_config import setup_logger

logger = setup_logger("ecommerce_data_lake.incremental")


@dataclass
class BatchRecord:
    batch_id: str
    status: str  # 'COMMITTED', 'IN_PROGRESS', 'FAILED'
    started_at: str
    completed_at: Optional[str] = None
    records_read: int = 0
    records_silver: int = 0
    records_quarantine: int = 0
    high_watermark: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class StateManifest:
    version: str = "1.0.0"
    last_updated: str = ""
    global_watermark: Optional[str] = None
    batches: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class IncrementalStateManager:
    """Maintains pipeline state manifest to ensure exactly-once batch semantics and idempotent reruns."""

    def __init__(self, state_dir: str) -> None:
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_file = self.state_dir / "pipeline_manifest.json"
        self.manifest = self._load_manifest()

    def _load_manifest(self) -> StateManifest:
        if self.manifest_file.is_file():
            try:
                with open(self.manifest_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return StateManifest(
                        version=data.get("version", "1.0.0"),
                        last_updated=data.get("last_updated", ""),
                        global_watermark=data.get("global_watermark"),
                        batches=data.get("batches", {}),
                    )
            except Exception as e:
                logger.warning(f"Could not read state manifest: {e}. Starting fresh state.")
        return StateManifest(last_updated=datetime.now(timezone.utc).isoformat())

    def _save_manifest(self) -> None:
        self.manifest.last_updated = datetime.now(timezone.utc).isoformat()
        temp_file = self.state_dir / f"pipeline_manifest.tmp.{datetime.now().timestamp()}"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(asdict(self.manifest), f, indent=2)
        # Atomic file rename on POSIX / Windows
        if self.manifest_file.exists():
            self.manifest_file.unlink()
        temp_file.rename(self.manifest_file)

    def is_batch_committed(self, batch_id: str) -> bool:
        """Checks if a batch has already been successfully committed."""
        b_info = self.manifest.batches.get(batch_id)
        return bool(b_info and b_info.get("status") == "COMMITTED")

    def begin_batch(self, batch_id: str) -> None:
        """Registers a batch as IN_PROGRESS. If previously committed, logs warning for re-execution."""
        if self.is_batch_committed(batch_id):
            logger.warning(
                f"Batch {batch_id} was previously committed. Re-running will apply idempotent updates."
            )
        self.manifest.batches[batch_id] = asdict(
            BatchRecord(
                batch_id=batch_id,
                status="IN_PROGRESS",
                started_at=datetime.now(timezone.utc).isoformat(),
            )
        )
        self._save_manifest()
        logger.info(f"Batch {batch_id} status set to IN_PROGRESS")

    def commit_batch(
        self,
        batch_id: str,
        records_read: int,
        records_silver: int,
        records_quarantine: int,
        batch_watermark: Optional[str] = None,
    ) -> None:
        """Atomically marks a batch as COMMITTED and advances watermark."""
        now_iso = datetime.now(timezone.utc).isoformat()
        current = self.manifest.batches.get(batch_id, {})
        current.update({
            "status": "COMMITTED",
            "completed_at": now_iso,
            "records_read": records_read,
            "records_silver": records_silver,
            "records_quarantine": records_quarantine,
            "high_watermark": batch_watermark,
            "error_message": None,
        })
        self.manifest.batches[batch_id] = current

        # Advance global watermark if new watermark is later
        if batch_watermark:
            if not self.manifest.global_watermark or batch_watermark > self.manifest.global_watermark:
                self.manifest.global_watermark = batch_watermark

        self._save_manifest()
        logger.info(
            f"Batch {batch_id} safely COMMITTED: read={records_read}, "
            f"silver={records_silver}, quarantine={records_quarantine}, "
            f"watermark={self.manifest.global_watermark}"
        )

    def fail_batch(self, batch_id: str, error_message: str) -> None:
        """Marks a batch as FAILED without advancing watermark."""
        now_iso = datetime.now(timezone.utc).isoformat()
        current = self.manifest.batches.get(batch_id, {})
        current.update({
            "status": "FAILED",
            "completed_at": now_iso,
            "error_message": error_message,
        })
        self.manifest.batches[batch_id] = current
        self._save_manifest()
        logger.error(f"Batch {batch_id} marked as FAILED: {error_message}")
