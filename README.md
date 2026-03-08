# Salesforce Data Extractor

Extract Salesforce data via SOQL with automatic Bulk API 2.0 / REST API selection.

## Prerequisites

- [mise](https://mise.jdx.dev/) (installs Python 3.14, Node.js 22, Salesforce CLI)
- Salesforce org with `sf org login web --alias <org>` completed

## Setup

1. Install toolchain and dependencies:
   ```bash
   mise install
   mise run setup
   ```

2. Configure `config.yaml`:
   - Copy from `config-example.yaml` (done automatically by `mise run setup`)
   - Set `org_alias` to your Salesforce org alias
   - Add objects to extract in the `objects` section
   - Choose mode: `full` or `incremental`

3. Authenticate to Salesforce:
   ```bash
   mise run auth
   ```

## Usage

```bash
# Test extraction (limited records per object, uses verify_limit from config)
mise run verify

# Full extraction of all configured objects
mise run execute

# Direct Python invocation with overrides
python extract.py --config my-config.yaml --mode incremental --limit 10
```

## Configuration

`config.yaml` (see `config-example.yaml` for full reference):

```yaml
org_alias: "my-org"
output_dir: "./output"
mode: "full"              # "full" | "incremental"
verify_limit: 10          # record limit for mise run verify

objects:
  - name: Account
    fields:               # optional — omit to fetch all fields via describe()
      - Id
      - Name
      - LastModifiedDate

  - name: Custom_Object__c  # no fields = auto-discover all
```

## Modes

- **full**: Extract all records for each configured object
- **incremental**: Extract only records modified since last run, tracked via `state.json` (requires `LastModifiedDate` field)

## API Selection

- **Default**: Bulk API 2.0 (handles large datasets efficiently)
- **Fallback**: REST API with automatic pagination (`query_all_iter`) if Bulk API fails
- **`--limit`**: Forces REST API (Bulk is unnecessary for small result sets)

## Output

```
output/<timestamp>/
  Account.csv
  Contact.csv
  Opportunity.csv
state.json                # incremental mode timestamps (per object)
extraction.log            # full debug log
```

## CLI Arguments

```
-c, --config    Path to config YAML (default: config.yaml)
-m, --mode      Override mode: full | incremental
-l, --limit     Limit records per object (adds SOQL LIMIT, forces REST API)
```

## Mise Tasks

| Task | Command | Description |
|------|---------|-------------|
| setup | `mise run setup` | Install pip deps, create config.yaml from example |
| auth | `mise run auth` | Login to SF org or verify existing session |
| execute | `mise run execute` | Full extraction of all configured objects |
| verify | `mise run verify` | Test extraction with `verify_limit` records per object |

## Project Structure

```
sf-extractor/
├── extract.py           # Main entry point
├── .mise.toml           # Toolchain (Python 3.14, Node 22, sf CLI) + tasks
├── config-example.yaml  # Configuration template
├── requirements.txt     # simple-salesforce, pyyaml
├── src/
│   ├── auth.py          # SF CLI credential extraction + simple-salesforce connect
│   ├── config.py        # YAML config parser (Config, ObjectConfig dataclasses)
│   ├── extractor.py     # Bulk API 2.0 / REST API extraction with fallback
│   ├── state.py         # Incremental state tracking (state.json)
│   └── writer.py        # CSV writer (removes SF attributes key)
├── output/              # Timestamped extraction output (gitignored)
└── extraction.log       # Debug log (gitignored)
```
