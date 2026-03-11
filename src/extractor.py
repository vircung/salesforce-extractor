"""Extract Salesforce data using Bulk API 2.0 with REST API fallback."""

import csv
import io
import logging
import time
from dataclasses import dataclass, field

from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceError

from .config import ObjectConfig

logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    object_name: str
    record_count: int = 0
    api_used: str = ""  # "bulk" or "rest"
    duration_seconds: float = 0.0
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.error is None


@dataclass
class ExtractionSummary:
    results: list[ExtractionResult] = field(default_factory=list)

    @property
    def succeeded(self) -> list[ExtractionResult]:
        return [r for r in self.results if r.success]

    @property
    def failed(self) -> list[ExtractionResult]:
        return [r for r in self.results if not r.success]


def _resolve_fields(sf: Salesforce, obj_config: ObjectConfig) -> list[str]:
    """Get field list: use configured fields or discover all via describe().

    Appends include fields (relationship fields) and deduplicates.
    """
    if obj_config.fields:
        fields = list(obj_config.fields)
    else:
        logger.info("No fields specified for %s, discovering via describe()", obj_config.name)
        desc = getattr(sf, obj_config.name).describe()
        fields = [f["name"] for f in desc["fields"]]
        logger.info("Discovered %d fields for %s", len(fields), obj_config.name)

    if obj_config.include:
        fields = list(dict.fromkeys(fields + obj_config.include))
        logger.info("Added %d include fields for %s", len(obj_config.include), obj_config.name)

    return fields


def _build_soql(
    object_name: str,
    fields: list[str],
    where_clause: str | None = None,
    limit: int | None = None,
) -> str:
    """Build a SOQL query string."""
    soql = f"SELECT {', '.join(fields)} FROM {object_name}"
    if where_clause:
        soql += f" WHERE {where_clause}"
    if limit:
        soql += f" LIMIT {limit}"
    return soql


def _extract_bulk(sf: Salesforce, object_name: str, soql: str) -> list[dict]:
    """Extract records using Bulk API 2.0. Returns list of record dicts."""
    logger.info("Bulk API 2.0 query for %s", object_name)
    bulk_handler = getattr(sf.bulk2, object_name)
    results = bulk_handler.query(soql)

    # Bulk API 2.0 returns CSV string(s); parse them into dicts
    records = []
    if isinstance(results, str):
        # Single CSV string
        reader = csv.DictReader(io.StringIO(results))
        records = list(reader)
    elif isinstance(results, list):
        # List of CSV strings (chunked)
        for chunk in results:
            if chunk.strip():
                reader = csv.DictReader(io.StringIO(chunk))
                records.extend(reader)
    else:
        raise ValueError(f"Unexpected Bulk API response type: {type(results)}")

    return records


def _extract_rest(sf: Salesforce, soql: str) -> list[dict]:
    """Extract records using REST API with automatic pagination."""
    logger.info("REST API query (fallback)")
    records = list(sf.query_all_iter(soql))
    return records


def _flatten_records(records: list[dict], relationship_fields: list[str]) -> list[dict]:
    """Flatten nested dicts from REST API to dot-notation keys.

    Bulk API records are already flat — detected and skipped.
    Null relationships are expanded to individual null values using
    the known relationship_fields list.
    """
    if not relationship_fields or not records:
        return records

    # Build prefix map: {"CreatedBy": ["CreatedBy.Name", "CreatedBy.Email"]}
    prefix_map: dict[str, list[str]] = {}
    for rf in relationship_fields:
        prefix = rf.split(".")[0]
        prefix_map.setdefault(prefix, []).append(rf)

    flattened = []
    for record in records:
        # Check if any value is a dict — if not, record is already flat
        has_nested = any(isinstance(v, dict) for v in record.values())
        if not has_nested:
            flattened.append(record)
            continue

        flat = {}
        for key, value in record.items():
            if key == "attributes":
                continue

            if key in prefix_map and isinstance(value, dict):
                # Flatten nested dict, extracting only listed fields
                _flatten_nested(flat, key, value, prefix_map[key])
            elif key in prefix_map and value is None:
                # Null relationship — expand to individual null values
                for rf in prefix_map[key]:
                    flat[rf] = None
            else:
                flat[key] = value

        flattened.append(flat)

    return flattened


def _flatten_nested(
    flat: dict, prefix: str, nested: dict, expected_fields: list[str],
) -> None:
    """Recursively flatten a nested dict, stripping 'attributes' keys."""
    for key, value in nested.items():
        if key == "attributes":
            continue
        dotted = f"{prefix}.{key}"
        if isinstance(value, dict):
            # Deeper nesting — recurse with sub-fields
            sub_fields = [f for f in expected_fields if f.startswith(dotted + ".")]
            if sub_fields:
                _flatten_nested(flat, dotted, value, sub_fields)
            else:
                flat[dotted] = value
        else:
            flat[dotted] = value


def extract_object(
    sf: Salesforce,
    obj_config: ObjectConfig,
    incremental_since: str | None = None,
    limit: int | None = None,
) -> tuple[ExtractionResult, list[dict]]:
    """Extract records for a single Salesforce object.

    Uses Bulk API 2.0 by default, falls back to REST API on failure.
    When limit is set, adds SOQL LIMIT clause and forces REST API
    (Bulk API is unnecessary for small result sets).
    Returns (result metadata, list of records).
    """
    result = ExtractionResult(object_name=obj_config.name)
    start = time.time()

    try:
        fields = _resolve_fields(sf, obj_config)

        # Ensure LastModifiedDate is included for incremental tracking
        if incremental_since and "LastModifiedDate" not in fields:
            fields.append("LastModifiedDate")

        where = None
        if incremental_since:
            where = f"LastModifiedDate > {incremental_since}"

        soql = _build_soql(obj_config.name, fields, where, limit=limit)
        logger.info("SOQL: %s", soql)

        # Use REST directly when limit is set (Bulk is overkill for small sets)
        if limit:
            records = _extract_rest(sf, soql)
            result.api_used = "rest"
        else:
            # Try Bulk API 2.0 first, fall back to REST on failure
            try:
                records = _extract_bulk(sf, obj_config.name, soql)
                result.api_used = "bulk"
            except (SalesforceError, Exception) as bulk_err:
                logger.warning(
                    "Bulk API failed for %s: %s. Falling back to REST API.",
                    obj_config.name, bulk_err,
                )
                records = _extract_rest(sf, soql)
                result.api_used = "rest"

        # Flatten nested dicts from REST API (no-op for Bulk API flat records)
        relationship_fields = [f for f in fields if '.' in f]
        if relationship_fields:
            records = _flatten_records(records, relationship_fields)

        result.record_count = len(records)
        result.duration_seconds = time.time() - start

        logger.info(
            "Extracted %d records from %s via %s API in %.1fs",
            result.record_count, obj_config.name, result.api_used, result.duration_seconds,
        )
        return result, records

    except Exception as e:
        result.error = str(e)
        result.duration_seconds = time.time() - start
        logger.error("Failed to extract %s: %s", obj_config.name, e)
        return result, []
