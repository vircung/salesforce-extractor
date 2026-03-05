"""Manage incremental extraction state (last run timestamps per object)."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

STATE_FILE = "state.json"


class ExtractionState:
    """Tracks last successful extraction timestamp per object."""

    def __init__(self, state_dir: Path):
        self.path = state_dir / STATE_FILE
        self.last_run: dict[str, str] = {}
        self._load()

    def _load(self):
        if self.path.exists():
            with open(self.path) as f:
                data = json.load(f)
            self.last_run = data.get("last_run", {})
            logger.info("Loaded state from %s (%d objects)", self.path, len(self.last_run))
        else:
            logger.info("No state file found at %s, starting fresh", self.path)

    def get_last_run(self, object_name: str) -> str | None:
        """Return the ISO timestamp of the last extraction for an object, or None."""
        return self.last_run.get(object_name)

    def update(self, object_name: str, timestamp: str | None = None):
        """Update the last run timestamp for an object and persist to disk."""
        if timestamp is None:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000+0000")
        self.last_run[object_name] = timestamp
        self._save()

    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w") as f:
            json.dump({"last_run": self.last_run}, f, indent=2)
        logger.debug("State saved to %s", self.path)
