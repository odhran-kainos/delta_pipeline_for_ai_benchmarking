# Contract: Bronze Transactions Pipeline

**Version**: 1.0.0  
**Last Updated**: 2025-11-18

## Inputs

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| data_sources.transactions.path | STRING | Yes | Filesystem or cloud URI pointing to JSON batch |
| data_sources.transactions.format | STRING | Yes | Currently `json`; supports future formats |
| data_sources.transactions.schema_enforcement | BOOLEAN | Yes | Enables explicit schema loading when true |
| run_id | STRING | Yes | Unique identifier supplied by orchestrator for lineage |
| spark_config | MAP | Optional | Overrides from `config/spark_config.yaml` |

### Source Payload Contract
- Format: newline-delimited JSON or single JSON array
- Required keys: `transaction_id`
- Optional keys: `customer_id`, `event_timestamp`, `amount`, `currency`
- Additional keys: Ignored but logged when schema evolution is enabled

## Processing Guarantees
- Schema enforcement using explicit `StructType`
- Deduplication on `transaction_id`, keeping row with latest `_ingest_ts`
- Validation rejects when `transaction_id` is null or empty
- Quarantine retention: 30 days with `_expires_at` column populated
- Metrics emitted: `rows_raw`, `rows_invalid`, `rows_loaded`, `ingestion_duration_seconds`, `dedupe_dropped`

## Outputs

### Bronze Delta Table (`data/bronze/transactions`)
- Partitioned by `_ingest_date`
- Append-only writes (merge optional for recovery with same schema)
- Schema documented in [data-model.md](../data-model.md)

### Quarantine Delta Table (`data/quarantine/transactions`)
- Contains rejected rows with rejection metadata
- Purged automatically or manually after 30 days

### Structured Logs
- JSON payload emitted at INFO level with metrics, run ID, and config snapshot (non-sensitive)
- WARN-level entry when validation rejects occur

## Failure Modes
- Missing configuration → raises `ValueError` prior to Spark read
- Schema mismatch → raises `AnalysisException` with actionable message
- File read failure → logs error and aborts without writing partial results

## Success Criteria Alignment
- Ensures SLA: ingestion completes within 15 minutes for 5 GB batches
- Guarantees count reconciliation: `rows_raw == rows_loaded + rows_invalid`
- Quarantine availability within 5 minutes post-run for all rejects
- Structured metrics produced ≥95% of runs
