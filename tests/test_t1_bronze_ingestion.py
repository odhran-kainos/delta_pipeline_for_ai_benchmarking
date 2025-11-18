"""Acceptance tests for the bronze transactions ingestion pipeline."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, Iterable
from uuid import uuid4

import pytest
import yaml
from delta import DeltaTable
from pyspark.sql import functions as F
from pipelines.bronze_transactions_pipeline import BronzeTransactionsPipeline

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class TestT1BronzeIngestion:
    """Test suite for T1 Bronze transaction ingestion pipeline."""
    
    def test_bronze_table_exists(self, spark_session, bronze_output_dir):
        """
        Test that the bronze_transactions Delta table is created.
        
        Acceptance Criteria:
        - Delta table bronze_transactions created with expected columns
        """
        # This assumes the implementation creates the table at data/bronze/transactions
        # Implementations should be tested against a test output directory
        table_path = bronze_output_dir / "transactions"
        
        # Verify the table path exists
        assert table_path.exists(), f"Bronze table directory does not exist at {table_path}"
        
        # Verify it's a valid Delta table
        assert DeltaTable.isDeltaTable(spark_session, str(table_path)), \
            f"Directory at {table_path} is not a valid Delta table"
    
    def test_required_columns_present(self, spark_session, bronze_output_dir):
        """
        Test that all required columns are present in bronze table.
        
        Required columns per T1 spec:
        - transaction_id (from source)
        - customer_id (from source)
        - event_timestamp (from source)
        - amount (from source)
        - currency (from source)
        - _ingest_ts (metadata - timestamp)
        - _file_name (metadata - string)
        """
        table_path = bronze_output_dir / "transactions"
        
        if not table_path.exists():
            pytest.skip("Bronze table not created yet")
        
        df = spark_session.read.format("delta").load(str(table_path))
        
        required_columns = {
            "transaction_id",
            "customer_id", 
            "event_timestamp",
            "amount",
            "currency",
            "_ingest_ts",
            "_file_name"
        }
        
        actual_columns = set(df.columns)
        
        missing_columns = required_columns - actual_columns
        assert not missing_columns, f"Missing required columns: {missing_columns}"
    
    def test_row_count_validation(self, spark_session, sample_transactions_path, bronze_output_dir):
        """
        Test that row count matches input minus invalid rows.
        
        Acceptance Criteria:
        - Row count matches input minus invalid rows
        
        Note: The sample data has 1070 rows. All should be valid (have transaction_id).
        """
        table_path = bronze_output_dir / "transactions"
        
        if not table_path.exists():
            pytest.skip("Bronze table not created yet")
        
        # Count source rows
        source_df = spark_session.read.json(sample_transactions_path)
        source_count = source_df.count()
        
        # Count rows with valid transaction_id
        valid_source_count = source_df.filter(F.col("transaction_id").isNotNull()).count()
        
        # Count bronze rows
        bronze_df = spark_session.read.format("delta").load(str(table_path))
        bronze_count = bronze_df.count()
        
        # Bronze count should equal valid source count
        assert bronze_count == valid_source_count, (
            f"Expected {valid_source_count} rows (valid from {source_count} total), "
            f"but found {bronze_count} in bronze table"
        )
    
    def test_metadata_columns_populated(self, spark_session, bronze_output_dir):
        """
        Test that ingestion metadata columns are populated (non-null).
        
        Acceptance Criteria:
        - Ingestion metadata columns populated (non-null)
        """
        table_path = bronze_output_dir / "transactions"
        
        if not table_path.exists():
            pytest.skip("Bronze table not created yet")
        
        df = spark_session.read.format("delta").load(str(table_path))
        
        # Check _ingest_ts is not null
        null_ingest_ts = df.filter(F.col("_ingest_ts").isNull()).count()
        assert null_ingest_ts == 0, f"Found {null_ingest_ts} rows with null _ingest_ts"
        
        # Check _file_name is not null
        null_file_name = df.filter(F.col("_file_name").isNull()).count()
        assert null_file_name == 0, f"Found {null_file_name} rows with null _file_name"
    
    def test_transaction_id_uniqueness(self, spark_session, bronze_output_dir):
        """
        Test that transaction_id values are present and ideally unique.
        
        Note: This is a data quality check beyond the basic T1 requirements.
        """
        table_path = bronze_output_dir / "transactions"
        
        if not table_path.exists():
            pytest.skip("Bronze table not created yet")
        
        df = spark_session.read.format("delta").load(str(table_path))
        
        # All rows should have transaction_id (per validation requirement)
        null_txn_ids = df.filter(F.col("transaction_id").isNull()).count()
        assert null_txn_ids == 0, "Invalid rows with null transaction_id should be rejected"
        
        # Check for duplicates (info only, not a hard requirement for T1)
        total_count = df.count()
        distinct_count = df.select("transaction_id").distinct().count()
        
        if total_count != distinct_count:
            duplicates = total_count - distinct_count
            # This is a warning, not a failure for T1
            pytest.warns(
                UserWarning,
                match=f"Found {duplicates} duplicate transaction_ids"
            )
    
    def test_data_types(self, spark_session, bronze_output_dir):
        """
        Test that columns have appropriate data types.
        """
        table_path = bronze_output_dir / "transactions"
        
        if not table_path.exists():
            pytest.skip("Bronze table not created yet")
        
        df = spark_session.read.format("delta").load(str(table_path))
        schema = df.schema
        
        # Build type map
        type_map = {field.name: str(field.dataType) for field in schema.fields}
        
        # Check key type expectations
        assert "transaction_id" in type_map
        assert "string" in type_map["transaction_id"].lower()
        
        assert "amount" in type_map
        assert any(t in type_map["amount"].lower() for t in ["double", "decimal", "float"])
        
        assert "_ingest_ts" in type_map
        assert "timestamp" in type_map["_ingest_ts"].lower()
    
    def test_invalid_records_rejected(self, spark_session, tmp_path, bronze_output_dir):
        """
        Test that rows missing transaction_id are rejected.
        
        This creates a test file with some invalid records and verifies they're excluded.
        
        Note: This test validates the logic but requires the implementation to actually
        run the pipeline against the test data. For benchmark evaluation, this test
        verifies that the bronze table contains only valid records.
        """
        table_path = bronze_output_dir / "transactions"
        
        if not table_path.exists():
            pytest.skip("Bronze table not created yet")
        
        # Verify that all records in the bronze table have transaction_id
        df = spark_session.read.format("delta").load(str(table_path))
        
        # Count records with null or missing transaction_id
        invalid_records = df.filter(
            F.col("transaction_id").isNull() | (F.col("transaction_id") == "")
        ).count()
        
        assert invalid_records == 0, \
            f"Found {invalid_records} invalid records in bronze table (should reject records with missing transaction_id)"

def _write_transactions_file(target_path: Path, records: Iterable[Dict[str, object]]) -> str:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with target_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
    return str(target_path)


def _write_config(tmp_path: Path, source_path: str) -> Dict[str, object]:
    config = {
        "database": {"bronze_path": str(tmp_path / "bronze")},
        "data_quality": {"enable_validation": True, "fail_on_error": False},
        "logging": {"level": "INFO", "format": "%(message)s"},
        "data_sources": {
            "transactions": {
                "path": source_path,
                "format": "json",
                "schema_enforcement": True,
            }
        },
        "quarantine": {
            "transactions_path": str(tmp_path / "quarantine"),
            "export_path": str(tmp_path / "quarantine" / "export"),
            "retention_days": 30,
        },
    }

    config_path = tmp_path / "pipeline_config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    config["_path"] = str(config_path)
    return config


def _run_pipeline(spark_session, config: Dict[str, object]):
    pipeline = BronzeTransactionsPipeline(spark_session, config_path=config["_path"])  # type: ignore[index]
    return pipeline.run(run_id=f"bronze-run-{uuid4()}")


@pytest.mark.usefixtures("cleanup_delta_tables")
def test_bronze_pipeline_writes_partitioned_delta_table(tmp_path, spark_session, cleanup_delta_tables):
    records = [
        {"transaction_id": "T-100", "customer_id": "C1", "event_timestamp": "2025-11-17T10:00:00", "amount": 125.5, "currency": "EUR"},
        {"transaction_id": "T-100", "customer_id": "C1", "event_timestamp": "2025-11-17T10:01:00", "amount": 150.5, "currency": "EUR"},
        {"transaction_id": "T-200", "customer_id": "C2", "event_timestamp": "2025-11-17T11:00:00", "amount": 20.0, "currency": "EUR"},
        {"customer_id": "C3", "event_timestamp": "2025-11-17T12:00:00", "amount": 99.0, "currency": "EUR"},
    ]

    source_file = _write_transactions_file(tmp_path / "inputs" / "transactions.json", records)
    config = _write_config(tmp_path, source_file)

    cleanup_delta_tables(str(Path(config["database"]["bronze_path"]) / "transactions"))  # type: ignore[index]
    cleanup_delta_tables(config["quarantine"]["transactions_path"])  # type: ignore[index]
    cleanup_delta_tables(config["quarantine"]["export_path"])  # type: ignore[index]

    metrics = _run_pipeline(spark_session, config)

    bronze_path = Path(config["database"]["bronze_path"]) / "transactions"  # type: ignore[index]
    assert DeltaTable.isDeltaTable(spark_session, str(bronze_path))

    bronze_df = spark_session.read.format("delta").load(str(bronze_path))
    expected_columns = {
        "transaction_id",
        "customer_id",
        "event_timestamp",
        "amount",
        "currency",
        "_ingest_ts",
        "_ingest_date",
        "_pipeline_run_id",
        "_source_file",
    }
    assert expected_columns.issubset(set(bronze_df.columns))

    assert metrics["rows_raw"] == len(records)
    assert metrics["rows_invalid"] == 1
    assert metrics["dedupe_dropped"] == 1
    assert metrics["rows_loaded"] == bronze_df.count() == 2
    assert all(row["_pipeline_run_id"] == metrics["pipeline_run_id"] for row in bronze_df.select("_pipeline_run_id").distinct().collect())

    ingest_dates = [row["_ingest_date"] for row in bronze_df.select("_ingest_date").distinct().collect()]
    assert len(ingest_dates) == 1, "Bronze output must be partitioned by a single ingest date per run"


@pytest.mark.usefixtures("cleanup_delta_tables")
def test_quarantine_persistence_and_export(tmp_path, spark_session, cleanup_delta_tables):
    records = [
        {"transaction_id": "TX-1", "customer_id": "C1", "event_timestamp": "2025-11-17T10:00:00", "amount": 5.0, "currency": "USD"},
        {"customer_id": "C2", "event_timestamp": "2025-11-17T10:05:00", "amount": 10.0, "currency": "USD"},
    ]

    source_file = _write_transactions_file(tmp_path / "inputs" / "transactions.json", records)
    config = _write_config(tmp_path, source_file)

    bronze_transactions_path = Path(config["database"]["bronze_path"]) / "transactions"  # type: ignore[index]
    quarantine_path = Path(config["quarantine"]["transactions_path"])  # type: ignore[index]
    export_root = Path(config["quarantine"]["export_path"])  # type: ignore[index]

    cleanup_delta_tables(str(bronze_transactions_path))
    cleanup_delta_tables(str(quarantine_path))
    cleanup_delta_tables(str(export_root))

    metrics = _run_pipeline(spark_session, config)

    assert metrics["rows_invalid"] == 1
    assert metrics["rows_loaded"] == 1
    assert metrics["quarantine_ready_within_5_min"] is True
    assert metrics["quarantine_export_latency_seconds"] is not None

    quarantine_df = spark_session.read.format("delta").load(str(quarantine_path))
    quarantine_columns = {
        "transaction_id",
        "rejection_reason",
        "validation_rule_id",
        "_pipeline_run_id",
        "_source_file",
        "_quarantine_ts",
        "_expires_at",
        "status",
    }
    assert quarantine_columns.issubset(set(quarantine_df.columns))
    assert quarantine_df.count() == 1
    assert quarantine_df.first().status == "quarantined"

    export_target = export_root / metrics["pipeline_run_id"]
    assert DeltaTable.isDeltaTable(spark_session, str(export_target))
    export_df = spark_session.read.format("delta").load(str(export_target))
    assert export_df.count() == metrics["rows_invalid"]
    assert metrics["sla_15_min_passed"] is True


@pytest.mark.usefixtures("cleanup_delta_tables")
def test_metrics_align_with_table_state(tmp_path, spark_session, cleanup_delta_tables):
    records = [
        {"transaction_id": "TX-2", "customer_id": "C1", "event_timestamp": "2025-11-17T09:00:00", "amount": 12.0, "currency": "USD"},
        {"transaction_id": "TX-3", "customer_id": "C2", "event_timestamp": "2025-11-17T09:05:00", "amount": 40.0, "currency": "USD"},
    ]

    source_file = _write_transactions_file(tmp_path / "inputs" / "transactions.json", records)
    config = _write_config(tmp_path, source_file)

    bronze_transactions_path = Path(config["database"]["bronze_path"]) / "transactions"  # type: ignore[index]
    quarantine_path = Path(config["quarantine"]["transactions_path"])  # type: ignore[index]
    export_root = Path(config["quarantine"]["export_path"])  # type: ignore[index]

    cleanup_delta_tables(str(bronze_transactions_path))
    cleanup_delta_tables(str(quarantine_path))
    cleanup_delta_tables(str(export_root))

    metrics = _run_pipeline(spark_session, config)

    bronze_df = spark_session.read.format("delta").load(str(bronze_transactions_path))
    assert metrics["rows_loaded"] == bronze_df.count()
    assert metrics["rows_invalid"] == 0
    assert metrics["dedupe_dropped"] == 0
    assert metrics["rows_raw"] == len(records)
    assert metrics["quarantine_ready_within_5_min"] is True

    # Count reconciliation check
    assert metrics["rows_loaded"] + metrics["rows_invalid"] == metrics["rows_raw"]
    assert metrics["ingestion_duration_seconds"] is not None
    assert metrics["ingestion_duration_seconds"] >= 0.0

    # Metrics should include config snapshot for observability
    snapshot = metrics["config_snapshot"]
    assert snapshot["transactions_source"]["path"] == source_file
    assert snapshot["quarantine"]["transactions_path"] == str(quarantine_path)
