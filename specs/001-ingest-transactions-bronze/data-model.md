# Data Model: Bronze Transactions Ingestion

**Last Updated**: 2025-11-18

## Entities

### 1. TransactionRawRecord (Bronze Table)
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| transaction_id | STRING | NO | Unique identifier from source payload; dedupe key |
| customer_id | STRING | YES | Source customer reference |
| event_timestamp | TIMESTAMP | YES | Event time parsed from source, preserves timezone info |
| amount | DOUBLE | YES | Monetary amount after numeric cast |
| currency | STRING | YES | ISO currency code |
| _ingest_ts | TIMESTAMP | NO | Pipeline processing timestamp |
| _ingest_date | DATE | NO | Partition column derived from `_ingest_ts` |
| _pipeline_run_id | STRING | NO | Unique identifier of pipeline execution |
| _source_file | STRING | NO | Fully qualified path of ingested file |

**Partitioning**: `_ingest_date`  
**Primary Key (logical)**: `transaction_id`  
**Retention**: Indefinite (append-only bronze history)

### 2. QuarantineRecord
| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| transaction_id | STRING | YES | Original value; may be null or duplicate |
| customer_id | STRING | YES | Copied from source |
| event_timestamp | TIMESTAMP | YES | Copied from source |
| amount | DOUBLE | YES | Copied from source |
| currency | STRING | YES | Copied from source |
| rejection_reason | STRING | NO | Validation failure code/message |
| validation_rule_id | STRING | NO | Identifier for rule (e.g., `missing_transaction_id`) |
| _pipeline_run_id | STRING | NO | Run emitting the record |
| _source_file | STRING | NO | Origin file path |
| _quarantine_ts | TIMESTAMP | NO | Timestamp of quarantine write |
| _expires_at | TIMESTAMP | NO | Timestamp for 30-day retention cutoff |
| status | STRING | NO | Lifecycle status (`quarantined`, `exported`, `purged`) |

**Partitioning**: `_quarantine_ts` (daily)  
**Retention**: Purged 30 days after `_quarantine_ts` or when `status` transitions to `purged`

### 3. IngestionRunMetrics (Structured Log Payload)
| Field | Type | Description |
|-------|------|-------------|
| pipeline_name | STRING | e.g., `BronzeTransactionsPipeline` |
| pipeline_run_id | STRING | UUID or timestamp-based identifier |
| started_at | TIMESTAMP | Run start time |
| completed_at | TIMESTAMP | Run end time |
| duration_seconds | DOUBLE | Rounded duration |
| rows_raw | LONG | Count before validation |
| rows_invalid | LONG | Rejected row count |
| rows_loaded | LONG | Rows written to bronze |
| source_file_count | LONG | Number of files processed |
| source_bytes | LONG | Total file size (optional) |
| dedupe_dropped | LONG | Rows removed during deduplication |
| quarantine_path | STRING | Delta table location for invalid rows |

## Relationships

- `TransactionRawRecord.transaction_id` has a 1:1 logical mapping with `QuarantineRecord.transaction_id` when the record was rejected; dedupe logic ensures only one surviving bronze record per ID.
- `IngestionRunMetrics.pipeline_run_id` joins to both tables via `_pipeline_run_id` for audit and troubleshooting.

## Validation Rules

1. `transaction_id` MUST be non-null and non-empty (primary rejection rule).  
2. Schema enforcement MUST succeed—unexpected columns logged but tolerated when schema evolution flag is enabled.  
3. Deduplication: Only the most recent `_ingest_ts` per `transaction_id` persists to bronze; others are counted in `dedupe_dropped`.

## Derived Columns

- `_ingest_date` derived from `date_trunc('DAY', _ingest_ts)` for partitioning.  
- `_expires_at` calculated as `_quarantine_ts + INTERVAL 30 DAYS` for retention automation.
