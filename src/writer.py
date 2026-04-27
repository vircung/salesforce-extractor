"""Write extracted Salesforce records to CSV files."""

import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def write_csv(records: list[dict], object_name: str, output_dir: Path) -> Path:
    """Write a list of record dicts to a CSV file.

    Uses the union of all record keys as fieldnames to ensure
    consistent columns. Returns the path to the written file.
    """
    if not records:
        logger.warning("No records to write for %s", object_name)
        return output_dir / f"{object_name}.csv"

    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / f"{object_name}.csv"

    fieldnames = list(dict.fromkeys(k for record in records for k in record))

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, restval="")
        writer.writeheader()
        writer.writerows(records)

    logger.info("Wrote %d records to %s", len(records), filepath)
    return filepath
