# Quickstart: Bronze Transactions Ingestion

Follow these steps to run and validate the bronze transactions pipeline in the local Delta Lake environment.

## Prerequisites
- Python 3.10.x managed via `pyenv`
- Java 11–17 installed and `JAVA_HOME` configured
- Dependencies from `requirements.txt` installed (`pip install -r requirements.txt`)
- Local directories under `data/` writable by the current user

## 1. Activate Environment & Launch Spark Session
```bash
pyenv activate delta-lakehouse
python -c "from pipelines.utils.spark_session import create_spark_session; spark = create_spark_session(); print(spark.version); spark.stop()"
```
Verify Spark reports version 3.5.x and Delta extensions are loaded.

## 2. Configure Source Paths
Update `config/pipeline_config.yaml`:
```yaml
data_sources:
  transactions:
    path: "data/raw_seed/transactions.json"
    format: "json"
    schema_enforcement: true
```
Ensure the bronze quarantine settings are defined:
```yaml
quarantine:
  transactions_path: "data/quarantine/transactions"
  retention_days: 30
```

## 3. Run the Pipeline
```bash
python - <<'PY'
from uuid import uuid4
from pipelines.utils.spark_session import create_spark_session
from pipelines.bronze_transactions_pipeline import BronzeTransactionsPipeline

spark = create_spark_session()
run_id = f"bronze-transactions-{uuid4()}"
try:
    pipeline = BronzeTransactionsPipeline(spark)
    metrics = pipeline.run(run_id=run_id)
    print(metrics)
finally:
    spark.stop()
PY
```
Expected output includes `rows_raw`, `rows_invalid`, `rows_loaded`, and `ingestion_duration_seconds`.

## 4. Verify Bronze Table
```bash
python - <<'PY'
from pipelines.utils.spark_session import create_spark_session

spark = create_spark_session()
try:
    bronze = spark.read.format("delta").load("data/bronze/transactions")
    bronze.show(5)
    print(f"Row count: {bronze.count()}")
finally:
    spark.stop()
PY
```
Confirm metadata columns `_ingest_ts`, `_ingest_date`, `_pipeline_run_id`, and `_source_file` are present.

## 5. Inspect Quarantine Records (If Any)
```bash
python - <<'PY'
from pipelines.utils.spark_session import create_spark_session

spark = create_spark_session()
try:
    quarantine = spark.read.format("delta").load("data/quarantine/transactions")
    quarantine.show(5)
finally:
    spark.stop()
PY
```
Verify rejected rows include `rejection_reason`, `_pipeline_run_id`, `_source_file`, `_quarantine_ts`, and `_expires_at`.

## 6. Run Tests
```bash
pytest tests/test_t1_bronze_ingestion.py -q
```
All tests should pass and validate table creation, schema, metadata, and metrics accuracy.

## 7. Maintenance Checklist
- Run `optimize_table` and `vacuum_table` after large ingestions (see `docs/best-practices-delta-lake-bronze-pipelines.md`).
- Purge quarantine records older than 30 days via scheduled job or manual vacuum.
- Monitor structured logs for metric anomalies or schema warnings.
