#!/usr/bin/env python3
"""Salesforce SOQL Data Extractor — main entry point."""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from src.auth import connect
from src.config import ExtractionMode, load_config
from src.extractor import ExtractionSummary, extract_approval_history, extract_object
from src.state import ExtractionState
from src.writer import write_csv

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"


def setup_logging(log_file: str = "extraction.log"):
    """Configure logging to console (INFO) and file (DEBUG)."""
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(LOG_FORMAT))
    root.addHandler(console)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(LOG_FORMAT))
    root.addHandler(fh)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract Salesforce data via SOQL")
    parser.add_argument(
        "-c", "--config",
        default="config.yaml",
        help="Path to config YAML file (default: config.yaml)",
    )
    parser.add_argument(
        "-m", "--mode",
        choices=[e.value for e in ExtractionMode],
        default=None,
        help="Override extraction mode from config",
    )
    parser.add_argument(
        "-l", "--limit",
        type=int,
        default=None,
        help="Limit number of records per object (SOQL LIMIT clause)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging()
    logger = logging.getLogger(__name__)

    total_start = time.time()
    logger.info("=== Salesforce Data Extraction Started ===")

    # Load config
    try:
        config = load_config(args.config)
    except (FileNotFoundError, ValueError) as e:
        logger.error("Config error: %s", e)
        sys.exit(1)

    # Allow CLI override of mode
    mode = ExtractionMode(args.mode) if args.mode else config.mode
    limit = args.limit
    logger.info("Mode: %s | Objects: %d | Output: %s", mode, len(config.objects), config.output_dir)
    if limit:
        logger.info("Record limit per object: %d", limit)

    # Authenticate
    try:
        sf = connect(config.org_alias)
    except RuntimeError as e:
        logger.error("Authentication failed: %s", e)
        sys.exit(1)

    # Prepare output directory
    run_output_dir = config.output_dir

    # Check for existing CSV files and prompt before overwriting
    existing_csvs = sorted(run_output_dir.glob("*.csv")) if run_output_dir.exists() else []
    if existing_csvs:
        if sys.stdin.isatty():
            logger.info("Existing CSV files in %s:", run_output_dir)
            for csv_file in existing_csvs:
                logger.info("  %s", csv_file.name)
            try:
                answer = input("Overwrite? [y/N]: ").strip().lower()
            except EOFError:
                answer = ""
            if answer not in ("y", "yes"):
                logger.info("Aborted by user.")
                sys.exit(0)
        else:
            logger.warning("Existing CSV files in %s will be overwritten (non-interactive mode).", run_output_dir)

    # Load state for incremental mode
    state = None
    if mode == "incremental":
        state_dir = Path(args.config).resolve().parent
        if state_dir != Path.cwd() and Path("state.json").exists():
            logger.warning(
                "Found state.json in CWD (%s) but state directory is now %s. "
                "Consider moving state.json to preserve incremental timestamps.",
                Path.cwd(), state_dir,
            )
        state = ExtractionState(state_dir)

    # Extract each object
    summary = ExtractionSummary()
    run_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000+0000")

    for obj_config in config.objects:
        logger.info("--- Extracting: %s ---", obj_config.name)

        incremental_since = None
        if state:
            incremental_since = state.get_last_run(obj_config.name)
            if incremental_since:
                logger.info("Incremental since: %s", incremental_since)
            else:
                logger.info("No previous state, doing full extract for %s", obj_config.name)

        result, records = extract_object(sf, obj_config, incremental_since, limit=limit)
        summary.results.append(result)

        if result.success and records:
            write_csv(records, obj_config.name, run_output_dir)
            if state:
                state.update(obj_config.name, timestamp=run_timestamp)

            # Extract approval history if configured (error-isolated)
            if obj_config.approval_history:
                try:
                    logger.info("--- Extracting approval history for: %s ---", obj_config.name)
                    ah_result, ah_records = extract_approval_history(sf, obj_config, limit=limit)
                    summary.results.append(ah_result)
                    if ah_result.success and ah_records:
                        write_csv(ah_records, f"ApprovalHistory_{obj_config.name}", run_output_dir)
                    elif ah_result.success and not ah_records:
                        logger.info("No approval history found for %s", obj_config.name)
                except Exception as e:
                    logger.error("Approval history extraction failed for %s: %s", obj_config.name, e)

        elif result.success and not records:
            logger.info("No records returned for %s", obj_config.name)

    # Print summary
    total_duration = time.time() - total_start
    logger.info("=== Extraction Complete ===")
    logger.info("Duration: %.1fs", total_duration)
    logger.info("Succeeded: %d | Failed: %d", len(summary.succeeded), len(summary.failed))

    for r in summary.succeeded:
        logger.info("  OK  %s: %d records (%s API, %.1fs)", r.object_name, r.record_count, r.api_used, r.duration_seconds)

    for r in summary.failed:
        logger.error("  FAIL %s: %s", r.object_name, r.error)

    if summary.failed:
        logger.warning("Some objects failed. Check logs for details.")
        sys.exit(2)


if __name__ == "__main__":
    main()
