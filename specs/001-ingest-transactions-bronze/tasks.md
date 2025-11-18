# Tasks: Bronze Transactions Ingestion

**Input**: Design documents from `/specs/001-ingest-transactions-bronze/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Update existing pytest suite (`tests/test_t1_bronze_ingestion.py`) as specified below.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: User story label from the specification (US1, US2, US3)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

- [x] T001 Update transactions source and quarantine defaults in `config/pipeline_config.yaml`.
- [x] T002 [P] Add `.gitkeep` placeholder for `data/quarantine/transactions/` to ensure repository tracking.

---

## Phase 2: Foundational (Blocking Prerequisites)

- [x] T003 Implement configuration validation helper in `pipelines/base_pipeline.py` to enforce required transactions and quarantine keys.
- [x] T004 [P] Extend `pipelines/utils/delta_operations.py` with a partition-aware append helper for bronze writes (supports optimize/vacuum hooks).

---

## Phase 3: User Story 1 - Ingest Raw Transactions Reliably (Priority: P1) 🎯 MVP

**Goal**: Load transaction JSON batches into `bronze_transactions` with explicit schema, metadata columns, and append-only semantics.

**Independent Test**: Run `BronzeTransactionsPipeline.run(run_id=...)` against sample data and confirm bronze table row count, schema, and metadata match expectations without inspecting other stories.

### Implementation

- [x] T005 [P] [US1] Update `pipelines/bronze_transactions_pipeline.py` to accept `run_id` in `run()` and initialize expanded metrics (rows, dedupe, timings, run identifiers).
- [x] T006 [P] [US1] Refactor `extract()` in `pipelines/bronze_transactions_pipeline.py` to validate config, load the explicit schema, cache the DataFrame, and capture `rows_raw` via single-pass aggregation.
- [x] T007 [US1] Enhance `transform()` in `pipelines/bronze_transactions_pipeline.py` to add `_ingest_ts`, `_ingest_date`, `_pipeline_run_id`, `_source_file`, apply transaction_id deduplication, and compute `rows_invalid`/`dedupe_dropped` metrics.
- [x] T008 [US1] Rewrite `load()` in `pipelines/bronze_transactions_pipeline.py` to append to `data/bronze/transactions` using the delta helper, update metrics, and trigger post-write optimization hooks.
- [x] T009 [US1] Update `tests/test_t1_bronze_ingestion.py` to verify bronze schema, metadata columns, deduplication behaviour, and row-count reconciliation against pipeline metrics.

**Checkpoint**: User Story 1 independently delivers reliable bronze ingestion with validated schema and metadata.

---

## Phase 4: User Story 2 - Monitor Ingestion Health (Priority: P2)

**Goal**: Provide actionable metrics and structured logging so operators can monitor pipeline performance without inspecting raw tables.

**Independent Test**: Execute the pipeline and confirm the returned metrics dictionary and structured logs match Delta table counts and include timing data.

### Implementation

- [x] T010 [P] [US2] Add aggregated metrics method in `pipelines/bronze_transactions_pipeline.py` to compute rows/invalid/dedupe counts in a single Spark action.
- [x] T011 [US2] Emit structured JSON metrics and configuration snapshot from `pipelines/bronze_transactions_pipeline.py` after successful runs.
	- [x] T012 [US2] Extend `tests/test_t1_bronze_ingestion.py` to assert metrics completeness (rows_raw, rows_invalid, rows_loaded, ingestion_duration_seconds, dedupe_dropped) and alignment with Delta queries.
	- [x] T019 [US2] Capture wall-clock runtime (start/stop timestamps) inside the pipeline run, derive SLA status fields (<=15 min) in the metrics payload, and surface them to logs/observability hooks.
	- [x] T020 [US2] Extend monitoring tests to validate SLA metrics: simulate a run, assert `ingestion_duration_seconds` and SLA flags are emitted, and confirm Prefect flow wiring propagates them.

**Checkpoint**: Operators can rely on returned metrics/logs to validate run health.

---

## Phase 5: User Story 3 - Investigate Invalid Transactions (Priority: P3)

**Goal**: Persist invalid transactions with rich context for remediation while keeping bronze history clean.

**Independent Test**: Inject rows missing `transaction_id`, run the pipeline, and confirm they appear only in the quarantine table with retention metadata and can be exported for upstream fixes.

### Implementation

	- [x] T013 [P] [US3] Implement quarantine writer in `pipelines/bronze_transactions_pipeline.py` to persist invalid rows with `rejection_reason`, `validation_rule_id`, `_quarantine_ts`, `_expires_at`, and `status` fields, and surface a sanitized `export_quarantine_records()` helper wired to config paths.
	- [x] T014 [US3] Integrate retention logic using `retention_days` config so `_expires_at` is populated, bronze writes exclude quarantined rows, and export helpers publish Delta files under `data/quarantine/transactions/export/` for upstream pickup.
	- [x] T015 [US3] Add quarantine-focused assertions to `tests/test_t1_bronze_ingestion.py` to verify invalid rows are stored with metadata, excluded from bronze counts, and available via the export helper/path for replay.
	- [x] T021 [US3] Add latency tracking to the export helper so it stamps `export_ready_ts`, calculates time since pipeline completion, and emits structured logs/metrics validating the 5-minute SLA.
	- [x] T022 [US3] Write tests that exercise the export helper, confirming `export_ready_ts` is recorded, latency metrics populate, and alerts/logs trigger when the 5-minute threshold is breached.

**Checkpoint**: Data quality analysts can triage invalid records using the quarantine table alone.

---

## Phase 6: Polish & Cross-Cutting Concerns

- [x] T016 [P] Document run_id usage, dedupe metrics, quarantine workflow, and export handoff steps (location, retention, replay expectations) in `docs/best-practices-delta-lake-bronze-pipelines.md`.
- [x] T017 Refresh Prefect orchestration example in `pipelines/orchestration/prefect_flows.py` to pass `run_id`, log metrics emitted by the bronze pipeline, and surface the configured quarantine export path for operators.
- [x] T018 Execute `pytest tests/test_t1_bronze_ingestion.py -q` and capture results in the feature log (commit message or PR notes).

---

## Dependencies & Execution Order

- **Setup (Phase 1)** → **Foundational (Phase 2)** → User Stories (Phases 3–5) → **Polish (Phase 6)**.
- Phase 3 (US1) depends on Phases 1–2; Phase 4 (US2) and Phase 5 (US3) depend on completion of Phase 2 and can proceed after US1 delivers the MVP.
- Testing tasks within each story depend on corresponding implementation tasks.

## Parallel Opportunities

- Setup task T002 can run alongside configuration work.
- T004 and T005 may be parallelized once configuration helper (T003) lands.
- After Foundational tasks, US2 logging instrumentation (T010–T012) and US3 quarantine work (T013–T015) can progress concurrently once US1 establishes the ingestion baseline.
- Final polish tasks T016–T018 can be executed in parallel, except T018 which should follow successful test enhancements.

## Implementation Strategy

1. Complete Setup and Foundational phases to unblock user stories.
2. Deliver US1 end-to-end as the MVP and validate via updated tests.
3. Layer US2 monitoring enhancements and US3 quarantine workflow, keeping stories independently testable.
4. Finish with polish tasks to align documentation, orchestration, and automated test execution.
