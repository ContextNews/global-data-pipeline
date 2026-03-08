"""Incremental update state tracking.

Persists per-source, per-indicator metadata so subsequent runs can skip
indicators that haven't changed.
"""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from global_data_pipeline.logging import get_logger

log = get_logger("storage.state")


class PipelineState:
    """Manages incremental state for a single source."""

    def __init__(self, state_dir: Path, source_name: str) -> None:
        self._path = state_dir / f"{source_name}.json"
        self._source = source_name
        self._data: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        if self._path.exists():
            with open(self._path) as f:
                return json.load(f)
        return {
            "source": self._source,
            "last_run": None,
            "indicators": {},
        }

    def save(self) -> None:
        self._data["last_run"] = datetime.now(timezone.utc).isoformat()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w") as f:
            json.dump(self._data, f, indent=2)
        log.debug("State saved to %s", self._path)

    @property
    def last_run(self) -> str | None:
        return self._data.get("last_run")

    def get_indicator(self, code: str) -> dict[str, Any] | None:
        return self._data["indicators"].get(code)

    def update_indicator(
        self,
        code: str,
        *,
        last_updated: str | None = None,
        row_count: int = 0,
        checksum: str | None = None,
    ) -> None:
        self._data["indicators"][code] = {
            "last_updated": last_updated,
            "row_count": row_count,
            "checksum": checksum,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

    def should_skip(self, code: str, current_last_updated: str | None) -> bool:
        """Return True if the indicator hasn't changed since last extraction."""
        prev = self.get_indicator(code)
        if prev is None:
            return False
        if current_last_updated is None:
            # No update timestamp available — can't skip
            return False
        return prev.get("last_updated") == current_last_updated

    def indicator_count(self) -> int:
        return len(self._data["indicators"])

    @staticmethod
    def compute_checksum(data: bytes) -> str:
        return f"sha256:{hashlib.sha256(data).hexdigest()}"
