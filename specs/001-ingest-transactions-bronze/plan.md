# Implementation Plan: Bronze Transactions Ingestion

**Branch**: `001-ingest-transactions-bronze` | **Date**: 2025-11-18 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-ingest-transactions-bronze/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

Implement a bronze-layer ingestion pipeline that reads JSON transaction batches, enforces the explicit schema, enriches rows with ingestion metadata, quarantines invalid records for 30 days, and writes deduplicated results to the `bronze_transactions` Delta table in append mode. The solution will leverage the existing `BasePipeline` abstraction, Spark 3.5.x with Delta Lake 3.2.0 (per project README), and reusable utilities in `pipelines/utils` to ensure idempotent writes, structured metrics emission, and observability aligned with the medallion constitution.

## Technical Context

<!--
  ACTION REQUIRED: Replace the content in this section with the technical details
  for the project. The structure here is presented in advisory capacity to guide
  the iteration process.
-->

**Language/Version**: Python 3.10.x (per README) running on Spark 3.5.2 with Delta Lake 3.2.0
**Primary Dependencies**: PySpark, Delta Lake, Prefect (for orchestration hooks), project utilities in `pipelines.utils`
**Storage**: Delta Lake tables on local filesystem paths under `data/bronze`, `data/quarantine`
**Testing**: pytest test suite (`tests/test_t1_bronze_ingestion.py`) with PySpark fixtures
**Target Platform**: macOS/Linux development environment with Java 11–17 and local filesystem-based Delta tables
**Project Type**: Single Python data engineering project (medallion pipelines)
**Performance Goals**: Complete 5 GB batch ingestion within 15 minutes; maintain zero discrepancy between source counts and bronze+quarantine totals
**Constraints**: Bronze layer must be append-only, maintain lineage metadata, and respect constitution retention/deduplication mandates
**Scale/Scope**: Daily synthetic transaction batches (tens of thousands of rows) with future cloud-ready alignment

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **Bronze Layer Immutability and Lineage**: Plan keeps append-only writes, adds `_ingest_ts`, `_ingest_date`, `_pipeline_run_id`, `_source_file`, and retains quarantine entries for 30 days — compliant.
- **Data Quality Gatekeeping**: Validation rule for `transaction_id`, quarantine persistence, and actionable error logging included — compliant.
- **Performance and Cost Efficiency**: Single-pass metrics, deduplication strategy, and partitioning by `_ingest_date` planned; optimize/vacuum scheduled post-load — compliant.
- **Schema and Configuration Governance**: Explicit schema class, config-driven source definitions, and defensive config validation — compliant.
- **Resilience, Testing, and Review Discipline**: Unit/integration tests in pytest, structured logging, and adherence to review gates — compliant.

## Implementation Phases

1. **Phase 0 – Research & Validation**
  - Finalize deduplication window logic and metrics aggregation approach (documented in `research.md`).
  - Confirm config schema additions for quarantine retention and validate against existing YAML structure.

2. **Phase 1 – Design & Contracts**
  - Update `data-model.md`, `contracts/bronze_transactions_pipeline_contract.md`, and `quickstart.md` to reflect metadata, quarantine rules, and runbook steps.
  - Align constitution check after design assets, ensuring no regressions in bronze principles.

3. **Phase 2 – Implementation**
  - Refactor `pipelines/bronze_transactions_pipeline.py`: add schema loader, config validation, metadata enrichment, deduplication window, export handler for quarantine records, single-pass metrics, append write, and quarantine handling.
  - Extend `pipelines/utils/delta_operations.py` if needed for shared dedupe/write helpers.
  - Update `config/pipeline_config.yaml` with transactions source/quarantine entries, including an explicit `quarantine_export_path` for upstream access.

4. **Phase 3 – Testing & Observability**
  - Enhance `tests/test_t1_bronze_ingestion.py` with scenarios covering dedupe, quarantine retention metadata, and metrics totals.
  - Add new tests for config validation and replay/export expectations if required.
  - Verify structured logging and Delta history reflect constitution requirements.

5. **Phase 4 – Optimization & Documentation**
  - Document optimize/vacuum cadence in `docs/best-practices-delta-lake-bronze-pipelines.md` (if updates needed).
  - Prepare runbook notes for orchestration (Prefect flow) and publish metrics dashboards alignment.
  - Capture export workflow and downstream handoff expectations (paths, refresh cadence) in `quickstart.md` and the bronze best-practices guide.

## Project Structure

### Documentation (this feature)

```text
specs/001-ingest-transactions-bronze/
├── plan.md              # Implementation plan (this file)
├── research.md          # Phase 0 research notes
├── data-model.md        # Phase 1 data definitions
├── quickstart.md        # Phase 1 runbook excerpt
├── contracts/           # Phase 1 interface contracts
└── checklists/          # Specification quality checklist (existing)
```

### Source Code (repository root)

```text
pipelines/
├── base_pipeline.py
├── bronze_transactions_pipeline.py      # Target implementation file
├── sample_etl_pipeline.py
├── utils/
│   ├── delta_operations.py
│   ├── spark_session.py
│   └── __init__.py
└── orchestration/
    └── prefect_flows.py

config/
├── pipeline_config.yaml                  # Source/destination paths
├── spark_config.yaml
└── data_sources_example.yaml

tests/
├── conftest.py                          # Spark session fixture
└── test_t1_bronze_ingestion.py          # Acceptance tests for bronze ingestion

data/
├── bronze/
├── silver/
├── gold/
└── raw_seed/

docs/
└── best-practices-delta-lake-bronze-pipelines.md
```
**Structure Decision**: Single Python data engineering repository. Work will modify `pipelines/bronze_transactions_pipeline.py`, extend utilities under `pipelines/utils`, update configuration in `config/pipeline_config.yaml`, and add supporting docs/tests under existing `tests` and `docs` trees.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| _None_ | _N/A_ | _N/A_ |
