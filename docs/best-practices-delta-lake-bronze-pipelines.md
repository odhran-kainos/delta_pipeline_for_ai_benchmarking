# Bronze Delta Lake Pipeline Best Practices

## Run Identification
- Supply a globally unique `run_id` for every ingestion so `_pipeline_run_id` can be used to trace downstream tables and quarantined records.
- Populate ingestion metadata consistently: `_ingest_ts` mirrors the pipeline start timestamp and `_ingest_date` partitions bronze outputs for incremental readers.

## Metrics and Observability
- Collect `rows_raw`, `rows_invalid`, `rows_loaded`, and `dedupe_dropped` from a single Spark action to avoid repeated scans.
- Record `started_at`, `completed_at`, and `ingestion_duration_seconds`; derive `sla_15_min_passed` to enforce the 15-minute SLA.
- Emit metrics as structured JSON in application logs alongside a sanitized `config_snapshot` containing source and quarantine locations.

## Deduplication Strategy
- Deduplicate on `transaction_id`, retaining the newest record by ordering on `_ingest_ts` and an ingestion sequence column for deterministic ties.
- Inspect `dedupe_dropped` to spot upstream churn; alert when unexpected spikes appear.

## Quarantine Workflow
- Reject rows missing `transaction_id`, stamp them with `rejection_reason`, `validation_rule_id`, `_quarantine_ts`, `_expires_at`, and `status`.
- Persist quarantined rows to `quarantine.transactions_path` in Delta format and keep them for the configured `retention_days` (default 30).
- Update runbooks to remind operators to monitor the quarantine table and vacuum expired records.

## Export and Handoff
- After each run, publish a sanitized Delta snapshot under `quarantine.export_path/<run_id>` so upstream owners can remediate data issues.
- Capture export latency (`quarantine_export_latency_seconds`) and confirm `quarantine_ready_within_5_min` to meet the five-minute availability SLA.
- Share the export location and remediation expectations with upstream teams; replays happen by resubmitting corrected files through the bronze pipeline.
