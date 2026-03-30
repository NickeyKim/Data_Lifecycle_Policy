# Data Lifecycle Policy

A Databricks Asset Bundle project that automatically identifies and truncates stale tables (90+ days without writes) in Unity Catalog, with built-in safety through DRY_RUN mode and exclusion lists.

> **[Korean version (한국어)](README_ko.md)**

## Overview

This solution scans managed tables within specified Unity Catalog catalogs, checks each table's Delta history for the last write operation, and identifies tables that haven't been written to in over 90 days. It supports two run modes:

- **DRY_RUN** (default): Scans and logs stale table candidates without making any changes
- **TRUNCATE**: Actually truncates the identified stale tables

All scan results and actions are recorded in governance tables for full audit trails.

## Architecture

```
system.information_schema.tables
         │
         ▼
  ┌──────────────┐     ┌──────────────────────┐
  │  List managed │────▶│  DESCRIBE HISTORY     │
  │  tables       │     │  (per table)          │
  └──────────────┘     └──────────┬───────────┘
                                  │
                    Check last write > 90 days?
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼              ▼
              ┌──────────┐ ┌──────────┐  ┌────────────┐
              │  SKIP    │ │ DRY_RUN  │  │  TRUNCATE  │
              │(excluded)│ │ (log     │  │  (execute & │
              │          │ │  only)   │  │   log)      │
              └──────────┘ └──────────┘  └────────────┘
                    │             │              │
                    └─────────────┼──────────────┘
                                  ▼
                     ┌────────────────────────┐
                     │  lifecycle_candidates  │
                     │  (audit log table)     │
                     └────────────────────────┘
```

## Job Configuration

The job uses a **for_each_task** pattern to iterate over multiple target catalogs in parallel.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `TARGET_CATALOGS` | *(required)* | Catalog name to scan (passed via `for_each_task` inputs) |
| `RUN_MODE` | `DRY_RUN` | `DRY_RUN` to log only, `TRUNCATE` to actually delete data |

### Staleness Criteria

- A table is considered **stale** if its last write operation (`WRITE`, `MERGE`, `UPDATE`, `DELETE`, `TRUNCATE`, `REPLACE TABLE AS SELECT`) occurred **more than 90 days ago**
- Tables starting with `__` are excluded from scanning
- Tables in `information_schema`, `default`, and `_data_classification` schemas are excluded

## Governance Tables

### `lifecycle_candidates` (Audit Log)

Records every scan result with planned and executed actions.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | STRING | Unique run identifier (timestamp-based) |
| `scanned_at` | TIMESTAMP | Scan execution timestamp |
| `fqtn` | STRING | Fully-qualified table name |
| `last_write_ts` | TIMESTAMP | Last write operation timestamp from Delta history |
| `last_op` | STRING | Last write operation type |
| `is_stale_90d` | BOOLEAN | Whether the table is stale (90+ days) |
| `action_planned` | STRING | Planned action: `DRY_RUN` / `TRUNCATE` / `SKIP` |
| `action_executed` | STRING | Execution result: `SUCCESS` / `FAILED` / `SKIPPED` |
| `message` | STRING | Additional context or error details |

### `lifecycle_exclusions` (Table-Level Exclusion List)

Tables listed here are always skipped regardless of staleness.

| Column | Type | Description |
|--------|------|-------------|
| `table_catalog` | STRING | Catalog name |
| `table_schema` | STRING | Schema name |
| `table_name` | STRING | Table name |
| `reason` | STRING | Reason for exclusion |
| `updated_at` | TIMESTAMP | When the exclusion was added |

### `lifecycle_exclusions_schema` (Schema-Level Exclusion List)

Entire schemas to exclude from scanning.

| Column | Type | Description |
|--------|------|-------------|
| `table_catalog` | STRING | Catalog name |
| `table_schema` | STRING | Schema name |
| `reason` | STRING | Reason for exclusion |
| `updated_at` | TIMESTAMP | When the exclusion was added |

## Project Structure

```
data_lifecycle_policy/
├── databricks.yml                              # DAB bundle config (dev/prod targets)
├── resources/
│   └── data_lifecycle_policy.job.yml           # Job definition (for_each_task over catalogs)
├── src/
│   └── Data Lifecycle Policy.py                # Main lifecycle policy notebook
└── scratch/
    └── exploration.ipynb                       # Exploration notebook
```

## Getting Started

### Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html) v0.18+
- A Databricks Workspace with Unity Catalog enabled
- Read access to `system.information_schema.tables`
- Permission to run `DESCRIBE HISTORY` and `TRUNCATE TABLE` on target tables

### Configuration

1. Set up Databricks CLI authentication:
   ```bash
   databricks configure
   ```

2. Update the workspace host in `databricks.yml`:
   ```yaml
   workspace:
     host: https://<your-workspace>.cloud.databricks.com
   ```

3. Update the notebook to match your environment:
   - `CANDIDATE_TABLE` — where scan results are stored
   - `EXCLUSION_TABLE` — where exclusion lists are maintained
   - `EXCLUDE_SCHEMAS` — schemas to skip
   - `for_each_task` inputs in `data_lifecycle_policy.job.yml` — target catalog list

### Deploy & Run

```bash
# Deploy to development
databricks bundle deploy --target dev

# Run in DRY_RUN mode (default, safe)
databricks bundle run data_lifecycle_policy --target dev

# Run in TRUNCATE mode (destructive — actually deletes data)
# Update RUN_MODE parameter to "TRUNCATE" in the job configuration
```

## Safety Features

- **DRY_RUN by default**: No data is deleted unless `RUN_MODE` is explicitly set to `TRUNCATE`
- **Table-level exclusion list**: Protect specific tables from ever being truncated
- **Schema-level exclusion list**: Protect entire schemas from scanning
- **Full audit trail**: Every scan and action is logged to `lifecycle_candidates`
- **System table filtering**: Internal catalogs and system schemas are automatically excluded

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Notes

- Tested on Databricks Serverless Notebooks
- `DESCRIBE HISTORY` is used to determine the last write timestamp; if no write operation is found, the latest commit timestamp is used as a fallback
- The job uses `for_each_task` to process multiple catalogs in parallel
