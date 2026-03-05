# Salesforce Data Extractor

Extract Salesforce data via SOQL with automatic Bulk API 2.0 / REST API selection.

## Installation

```bash
pip install -r requirements.txt
```

## Setup

1. Login to Salesforce:
   ```bash
   sf org login web --alias my-org
   ```

2. Configure `config.yaml`:
   - Set `org_alias` to your alias from step 1
   - Add objects to extract in the `objects` section
   - Choose mode: `full` or `incremental`

## Usage

```bash
# Basic run
python extract.py

# Custom config
python extract.py --config my-config.yaml

# Override mode
python extract.py --mode incremental

# Limit records (for testing)
python extract.py --limit 10
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
