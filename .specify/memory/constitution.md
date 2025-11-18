# Delta Lake Pipelines Constitution

## Core Principles

### I. Bronze Layer Immutability and Lineage
Bronze tables are append-only, capture raw data with full lineage, and never use destructive write modes. Every ingestion run must add metadata columns (`_ingest_ts`, `_ingest_date`, `_pipeline_run_id`, `_source_file`) and maintain deduplication keys to guarantee idempotency and traceability.

### II. Data Quality Gatekeeping
Pipelines enforce explicit schemas, validation rules, and quarantine flows. Invalid or suspicious records are never dropped silently; they are routed to Delta-backed quarantine tables with rejection reasons and run identifiers. Schema violations or missing mandatory fields abort the run unless an approved remediation path exists.

### III. Performance and Cost Efficiency
We minimize Spark actions, reuse cached DataFrames, and collect metrics in a single pass. Partitioning, Z-ORDER, and VACUUM strategies are mandatory for every Delta table. Resource usage and runtime metrics (`rows_per_second`, stage durations) are logged as structured JSON for observability.

### IV. Schema and Configuration Governance
All schemas are versioned, defined explicitly in code, and evolved through controlled merges (`mergeSchema` only when approved). Configuration access uses defensive patterns with validation at startup. Paths are constructed with `pathlib.Path` to preserve portability across environments.

### V. Resilience, Testing, and Review Discipline
Each pipeline is decomposed into testable functions, covered by unit and integration tests across bronze, silver, and gold layers. Error handling wraps I/O and Spark operations with actionable messages. Code reviews verify compliance with this constitution, medallion architecture contracts, and data platform security requirements.

## Operational Standards for the Medallion Architecture

- **Bronze Layer**: Append/merge only, deduplicated on business keys plus ingestion timestamp, quarantine tables for invalid data, partitioned by `_ingest_date` at minimum.
- **Silver Layer**: Normalized, conformed data with business logic, idempotent transformations, and reconciliation checks against bronze counts. Schema evolution requires data contract approval.
- **Gold Layer**: Serving-ready aggregates and marts, optimized for consumption with freshness SLAs. Downstream tables document source lineage back to silver/bronze.
- **Observability**: Pipelines emit structured logs, Delta history is retained for 30 days, and table optimization jobs run on a documented cadence.
- **Security and Compliance**: Configuration secrets never reside in code, PII handling adheres to organizational policy, and access patterns respect least privilege.

## Development Workflow and Review Gates

1. **Design**: Engineers produce a lightweight design doc covering schema changes, validation rules, performance expectations, and downstream impact.
2. **Implementation**: Code adheres to SOLID Spark practices (narrow transformations preferred, broadcast joins explicit, no eager actions in transformations) and leverages shared utility libraries.
3. **Testing**: Mandatory unit tests for transformations, integration tests for Delta writes, and replay tests for idempotency. Test data mirrors edge cases (null keys, schema drift, duplicate payloads).
4. **Review**: Reviewers validate this constitution, confirm medallion contracts, ensure metrics/quarantine coverage, and request performance estimations. Any deviation requires an exception ticket with mitigation plan.
5. **Deployment**: Release notes include table changes, optimization strategy, and runbook updates. Canary or backfill plans accompany schema-breaking changes.

## Governance

This constitution supersedes conflicting style guides for PySpark medallion pipelines. Amendments require a design review, data governance approval, and migration plan for existing tables. Every pull request must state compliance status; reviewers block merges when violations are unresolved. Runtime engineers consult `docs/best-practices-delta-lake-bronze-pipelines.md` for detailed guidance.

**Version**: 1.0.0 | **Ratified**: 2025-11-18 | **Last Amended**: 2025-11-18
