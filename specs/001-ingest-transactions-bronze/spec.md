# Feature Specification: Bronze Transactions Ingestion

**Feature Branch**: `001-ingest-transactions-bronze`  
**Created**: 2025-11-18  
**Status**: Draft  
**Input**: User description: "Add Bronze ingestion for synthetic transactions dataset"

## User Scenarios & Testing *(mandatory)*

<!--
  IMPORTANT: User stories should be PRIORITIZED as user journeys ordered by importance.
  Each user story/journey must be INDEPENDENTLY TESTABLE - meaning if you implement just ONE of them,
  you should still have a viable MVP (Minimum Viable Product) that delivers value.
  
  Assign priorities (P1, P2, P3, etc.) to each story, where P1 is the most critical.
  Think of each story as a standalone slice of functionality that can be:
  - Developed independently
  - Tested independently
  - Deployed independently
  - Demonstrated to users independently
-->

### User Story 1 - Ingest Raw Transactions Reliably (Priority: P1)

Data engineers run the bronze ingestion pipeline so that new transaction files land in the bronze Delta table with consistent schema and metadata.

**Why this priority**: Without a dependable ingestion flow, downstream silver and gold layers cannot operate, blocking the entire medallion pipeline.

**Independent Test**: Trigger the pipeline with a representative JSON batch and confirm the bronze table contains the expected rows, schema, and metadata without manual intervention.

**Acceptance Scenarios**:

1. **Given** a configured transactions source path, **When** the pipeline executes successfully, **Then** a Delta table named `bronze_transactions` exists with the explicit schema and ingestion metadata columns populated for every row.
2. **Given** a raw file with records that conform to the schema, **When** the pipeline runs, **Then** the row count in bronze equals the source count minus rows rejected by validation.

---

### User Story 2 - Monitor Ingestion Health (Priority: P2)

Data platform operators review run metrics and logs to ensure pipeline performance, catch anomalies early, and meet observability standards.

**Why this priority**: Actionable metrics enable SLA tracking and troubleshooting without inspecting raw data manually.

**Independent Test**: Execute the pipeline, retrieve the metrics payload, and verify that all required counters and durations are present, accurate, and logged in structured form.

**Acceptance Scenarios**:

1. **Given** a completed pipeline run, **When** operators query the run metrics, **Then** they see `rows_raw`, `rows_invalid`, `rows_loaded`, and `ingestion_duration_seconds` with values matching audit queries on the Delta table and quarantine store.
2. **Given** a run that encounters validation rejects, **When** operators view the logs, **Then** the log includes warnings listing rejected row counts and references to the quarantine destination.

---

### User Story 3 - Investigate Invalid Transactions (Priority: P3)

Data quality analysts need access to rejected rows with context so they can remediate source issues and replay fixed data.

**Why this priority**: Capturing invalid data with root-cause context reduces data loss and accelerates feedback loops with source system owners.

**Independent Test**: Run the pipeline with intentionally malformed records and verify they appear in the quarantine store with metadata needed for triage.

**Acceptance Scenarios**:

1. **Given** a source file with records missing `transaction_id`, **When** the pipeline runs, **Then** those rows are excluded from bronze and appear in the quarantine dataset tagged with rejection reason, ingestion timestamp, and source file name.
2. **Given** a support request to replay previously invalid rows, **When** analysts query the quarantine store, **Then** they can identify affected records and corresponding pipeline run IDs needed for reprocessing.

---

[Add more user stories as needed, each with an assigned priority]

### Edge Cases

- Source file contains duplicate `transaction_id` values across multiple runs—pipeline must maintain idempotency and avoid double writes.
- Input JSON file includes unexpected columns or missing optional fields—schema enforcement should log discrepancies without breaking ingestion.
- Source path referenced in configuration is missing or inaccessible—pipeline must fail fast with actionable error messaging and skip writes.
- Individual records exceed size limits or contain invalid numeric formats—pipeline must capture parsing errors without halting the entire batch.

## Requirements *(mandatory)*

<!--
  ACTION REQUIRED: The content in this section represents placeholders.
  Fill them out with the right functional requirements.
-->

### Functional Requirements

- **FR-001**: Pipeline MUST read transactions from the configured source path and format, Fail the run when the schema can’t be loaded; document remediation instead of inferring at runtime.
- **FR-002**: Pipeline MUST apply the explicit transactions schema (transaction_id, customer_id, event_timestamp, amount, currency) and cast values to their target types before further processing.
- **FR-003**: Pipeline MUST append metadata columns `_ingest_ts`, `_ingest_date`, `_pipeline_run_id`, and `_source_file` to every record written to bronze.
- **FR-004**: Pipeline MUST validate that `transaction_id` is present and reject rows failing this rule without blocking valid records.
- **FR-005**: Pipeline MUST persist rejected rows to a Delta-backed quarantine dataset with rejection reason, run ID, and source file reference, retaining entries for 30 days before automated purge.
- **FR-006**: Pipeline MUST enforce idempotency by deduplicating records on `transaction_id`, retaining the record with the most recent `_ingest_ts` when duplicates exist.
- **FR-007**: Pipeline MUST write to `bronze_transactions` in append mode with partitioning by `_ingest_date`.
- **FR-008**: Pipeline MUST emit structured metrics (`rows_raw`, `rows_invalid`, `rows_loaded`, `ingestion_duration_seconds`) and log warnings when validation rejects occur.
- **FR-009**: Pipeline MUST fail fast with descriptive errors when configuration is missing, source data is unreadable, or schema enforcement cannot be satisfied.
- **FR-010**: Pipeline MUST expose an export of quarantine records for upstream systems to correct; replay of corrected data is initiated by the upstream system resubmitting cleaned files through the standard ingestion path.

### Key Entities *(include if feature involves data)*

- **TransactionRawRecord**: Represents a single transaction event after schema application; attributes include transaction_id, customer_id, event_timestamp, amount, currency, plus ingestion metadata columns.
- **IngestionRunMetrics**: Captures per-run metrics such as `rows_raw`, `rows_invalid`, `rows_loaded`, `ingestion_duration_seconds`, start/end timestamps, and run identifier.
- **QuarantineRecord**: Stores invalid or suspect transaction rows with rejection reason, validation rule ID, source file, pipeline run ID, and status flags for remediation.

### Assumptions & Dependencies

- Source transactions arrive as JSON files in cloud object storage accessible by the Spark cluster.
- Delta Lake 2.x+ features (OPTIMIZE, ZORDER, VACUUM) are available and governed by platform operations.
- Downstream silver pipelines expect `_ingest_date`, `_ingest_ts`, and `_pipeline_run_id` columns to drive incremental processing.
- Access to quarantine datasets is restricted to data quality and platform teams via existing governance controls.

## Success Criteria *(mandatory)*

<!--
  ACTION REQUIRED: Define measurable success criteria.
  These must be technology-agnostic and measurable.
-->

### Measurable Outcomes

- **SC-001**: 100% of bronze ingestion runs complete within 15 minutes for a 5 GB batch, including schema enforcement and validation.
- **SC-002**: Discrepancy between source row count and `rows_loaded + rows_invalid` is 0 for every run, verified via automated audit query.
- **SC-003**: Quarantine dataset entries are available with full context within 5 minutes of pipeline completion for all rejected rows.
- **SC-004**: At least 95% of pipeline runs produce structured logs and metrics consumable by monitoring systems without manual remediation.

