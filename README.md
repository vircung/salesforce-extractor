# Salesforce Data Extractor

Extract Salesforce data via SOQL with automatic Bulk API 2.0 / REST API selection.

## Setup

1. Install dependencies:
   ```bash
   mise run setup
   ```

2. Configure `config.yaml`:
   - Set `org_alias` to your Salesforce org alias
   - Add objects to extract in the `objects` section
   - Choose mode: `full` or `incremental`

3. Authenticate to Salesforce:
   ```bash
   mise run auth
   ```

## Usage

```bash
# Test extraction (limited records per object)
mise run verify

# Full extraction of all configured objects
mise run execute

# Advanced: Direct Python invocation
python extract.py --config my-config.yaml --mode incremental --limit 10
```

## Modes

- **full**: Extract all records
- **incremental**: Extract only records modified since last run (requires `LastModifiedDate`)

## Output

Data saved as CSV to:
```
output/<timestamp>/<object_name>.csv
```

Logs written to `extraction.log` (DEBUG) and console (INFO).
