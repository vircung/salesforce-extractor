"""Write extracted Salesforce records to CSV files."""

import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def write_csv(records: list[dict], object_name: str, output_dir: Path) -> Path:
    """Write a list of record dicts to a CSV file.

    Removes the Salesforce 'attributes' metadata key from each record.
    Returns the path to the written file.
    """
    if not records:
        logger.warning("No records to write for %s", object_name)
        return output_dir / f"{object_name}.csv"

    # Remove SF metadata and flatten OrderedDict → dict
    cleaned = []
    for record in records:
        row = {k: v for k, v in record.items() if k != "attributes"}
        cleaned.append(row)

    output_dir.mkdir(parents=True, exist_ok=True)
    filepath = output_dir / f"{object_name}.csv"

    fieldnames = list(cleaned[0].keys())

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(cleaned)

    logger.info("Wrote %d records to %s", len(cleaned), filepath)
    return filepath
