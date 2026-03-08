# AGENTS.md

> **Read `README.md` for full documentation.**

## Project
Python CLI to extract Salesforce data via SOQL. Bulk API 2.0 default, REST API fallback.

## Stack
Python 3.14 + Node 22 + sf CLI (via mise) | simple-salesforce, pyyaml

## Commands
```bash
mise install                                    # Install toolchain
mise run setup                                  # Install pip deps + create config.yaml
mise run auth                                   # Login / verify SF session
mise run execute                                # Full extraction
mise run verify                                 # Test extraction (limited records)
python extract.py --mode incremental --limit 5  # Direct invocation
```

## Structure
```
extract.py              # Entry point
src/
  auth.py               # sf org display → simple-salesforce connect
  config.py             # YAML config parser (Config, ObjectConfig)
  extractor.py          # Bulk API 2.0 / REST extraction + fallback
  state.py              # Incremental state (state.json)
  writer.py             # CSV writer
config-example.yaml     # Config template
.mise.toml              # Toolchain + tasks
```

## Critical Rules
- **NEVER commit config.yaml** — contains org alias, use config-example.yaml as template
- **Keep README.md updated** — sync docs when changing CLI args, features, or structure
- No tests or linting configured

## Quick Reference
| Task | Command |
|------|---------|
| Run | `mise run execute` |
| Verify | `mise run verify` |
| Auth | `mise run auth` |
| Debug | Check `extraction.log` |
