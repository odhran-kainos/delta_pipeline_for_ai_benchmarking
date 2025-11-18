# Research: Bronze Transactions Ingestion

**Prepared**: 2025-11-18  
**Objective**: Validate technical choices for the bronze transactions pipeline and collect guidance for implementation and observability.

## Research Tasks

1. Confirm Delta Lake best practices for append-only bronze tables with quarantine handling.  
2. Determine efficient Spark patterns for deduplication and single-pass metrics collection.  
3. Align retention, partitioning, and optimization routines with project constitution and existing docs.

## Findings

### 1. Bronze Table Write Strategy
- **Decision**: Use `mode="append"` for standard runs and expose optional merge (upsert) path leveraging `DeltaTable.merge` when recovering from partial loads.  
- **Rationale**: Preserves immutability, supports lineage, and aligns with constitution principle I. Delta Lake documentation recommends append for raw ingestion with optional merge for idempotent recovery.  
- **Implementation Notes**: Partition by `_ingest_date`, call `optimize_table` with Z-order on `transaction_id`, schedule `vacuum` with 7-day retention (per best-practices doc).

### 2. Deduplication & Idempotency
- **Decision**: Deduplicate on `transaction_id` retaining the row with the largest `_ingest_ts`.  
- **Rationale**: Spec-approved rule simplifies conflict resolution and ensures newest ingestion metadata persists.  
- **Implementation Notes**: Apply window function `row_number().over(Window.partitionBy("transaction_id").orderBy(col("_ingest_ts").desc()))` after metadata enrichment, filter to `row_num == 1`, then proceed to load.

### 3. Metrics Collection
- **Decision**: Cache validated DataFrame once and compute `rows_raw`, `rows_invalid`, `rows_loaded` via aggregations prior to writing.  
- **Rationale**: Avoids multiple `.count()` actions flagged in previous review and upholds constitution performance rules.  
- **Implementation Notes**: Use `df.agg()` to produce metrics in a single Spark job; persist metrics dict for logging and test assertions.

### 4. Quarantine Retention & Replay
- **Decision**: Persist invalid rows to `data/quarantine/transactions` for 30 days; upstream systems handle replay by resubmitting cleaned files.  
- **Rationale**: Balances auditability with storage management and matches spec clarifications.  
- **Implementation Notes**: Add scheduled cleanup script or document manual vacuum after 30 days; include rejection reason, source file, run ID, and timestamp columns.

### 5. Configuration & Observability
- **Decision**: Continue YAML-driven configuration with additive entries under `config/pipeline_config.yaml`.  
- **Rationale**: Maintains declarative approach and prevents hardcoded paths.  
- **Implementation Notes**: Validate config keys on initialization, emit structured JSON logs combining metrics and config seed values for traceability.

## References
- Project README (`README.md`): technology stack, Spark/Delta versions.  
- Constitution (`.specify/memory/constitution.md`): bronze immutability, validation, retention mandates.  
- Best practices doc (`docs/best-practices-delta-lake-bronze-pipelines.md`): guidance on append-only writes, deduplication windows, quarantine tables, optimization cadence.
