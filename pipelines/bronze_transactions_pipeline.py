import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from pipelines.base_pipeline import BasePipeline
from pipelines.utils.delta_operations import DeltaOperations


class BronzeTransactionsPipeline(BasePipeline):
    """Bronze layer ingestion pipeline for transactions data."""

    def __init__(self, spark, config_path: str = "config/pipeline_config.yaml"):
        super().__init__(spark, config_path)
        self.delta_ops = DeltaOperations(spark)
        self._raw_df: Optional[DataFrame] = None
        self._invalid_rows_df: Optional[DataFrame] = None
        self._run_id: Optional[str] = None
        self._run_started: Optional[datetime] = None
        self._quarantine_ready_ts: Optional[datetime] = None
        self.metrics: Dict[str, Any] = {}

        required_paths = [
            "database.bronze_path",
            "data_sources.transactions.path",
            "data_sources.transactions.format",
            "data_sources.transactions.schema_enforcement",
            "quarantine.transactions_path",
            "quarantine.export_path",
            "quarantine.retention_days",
        ]
        self.require_config_keys(self.config, required_paths)

        transactions_conf = self.config.get("data_sources", {}).get("transactions", {})
        self.transactions_path = transactions_conf.get("path")
        self.transactions_format = transactions_conf.get("format", "json")
        self.schema_enforcement = bool(transactions_conf.get("schema_enforcement", True))

        bronze_root = Path(self.config["database"]["bronze_path"])  # type: ignore[index]
        self.bronze_path = str(bronze_root / "transactions")

        quarantine_conf = self.config.get("quarantine", {})
        self.quarantine_path = str(Path(quarantine_conf["transactions_path"]))
        self.quarantine_export_path = str(Path(quarantine_conf["export_path"]))
        self.retention_days = int(quarantine_conf.get("retention_days", 30))

        Path(self.quarantine_path).mkdir(parents=True, exist_ok=True)
        Path(self.quarantine_export_path).mkdir(parents=True, exist_ok=True)

    def get_explicit_schema(self) -> StructType:
        """Define explicit schema for transactions data."""

        return StructType(
            [
                StructField("transaction_id", StringType(), False),
                StructField("customer_id", StringType(), True),
                StructField("event_timestamp", TimestampType(), True),
                StructField("amount", DoubleType(), True),
                StructField("currency", StringType(), True),
            ]
        )

    def _reset_state(self, run_id: str) -> None:
        self._run_id = run_id
        self._run_started = datetime.utcnow()
        self._quarantine_ready_ts = None
        self.metrics = {
            "pipeline_name": self.__class__.__name__,
            "pipeline_run_id": run_id,
            "started_at": self._run_started.isoformat(),
            "completed_at": None,
            "ingestion_duration_seconds": 0.0,
            "rows_raw": 0,
            "rows_invalid": 0,
            "rows_loaded": 0,
            "dedupe_dropped": 0,
            "quarantine_export_latency_seconds": None,
            "sla_15_min_passed": None,
            "quarantine_ready_within_5_min": None,
        }
        self.metrics["config_snapshot"] = {
            "transactions_source": {"path": self.transactions_path, "format": self.transactions_format},
            "quarantine": {
                "transactions_path": self.quarantine_path,
                "export_path": self.quarantine_export_path,
                "retention_days": self.retention_days,
            },
        }

    def extract(self) -> DataFrame:
        """Extract transaction data from configured source."""

        reader = self.spark.read.format(self.transactions_format)
        if self.schema_enforcement:
            reader = reader.schema(self.get_explicit_schema())

        raw_df = reader.load(self.transactions_path).cache()
        rows_raw = raw_df.agg(F.count(F.lit(1)).alias("rows_raw")).collect()[0]["rows_raw"] or 0
        self.metrics["rows_raw"] = int(rows_raw)
        self.logger.info("Extracted %s rows from %s", rows_raw, self.transactions_path)
        self._raw_df = raw_df
        return raw_df

    def transform(self, df: DataFrame) -> DataFrame:
        """Add metadata, deduplicate on transaction_id, and capture invalid rows."""

        if not self._run_started or not self._run_id:
            raise RuntimeError("Pipeline run_id not initialised. Call run() instead of individual stages.")

        ingest_ts_literal = F.lit(self._run_started).cast("timestamp")
        run_id_literal = F.lit(self._run_id)

        enriched = (
            df.withColumn("_pipeline_run_id", run_id_literal)
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_ingest_ts", ingest_ts_literal)
            .withColumn("_ingest_date", F.to_date("_ingest_ts"))
            .withColumn("_ingest_sequence", F.monotonically_increasing_id())
        )

        valid_candidates = enriched.filter(F.col("transaction_id").isNotNull())
        invalid_df = enriched.filter(F.col("transaction_id").isNull()).drop("_ingest_sequence")
        invalid_df = (
            invalid_df.withColumn("rejection_reason", F.lit("missing_transaction_id"))
            .withColumn("validation_rule_id", F.lit("missing_transaction_id"))
            .withColumn("_quarantine_ts", ingest_ts_literal)
            .withColumn(
                "_expires_at",
                F.col("_quarantine_ts") + F.expr(f"INTERVAL {int(self.retention_days)} DAYS"),
            )
            .withColumn("status", F.lit("quarantined"))
        )
        self._invalid_rows_df = invalid_df

        window = Window.partitionBy("transaction_id").orderBy(
            F.col("_ingest_ts").desc(),
            F.col("_ingest_sequence").desc(),
        )
        ranked = valid_candidates.withColumn("_row_number", F.row_number().over(window))

        metrics_counts = self._calculate_dedupe_metrics(ranked)
        valid_rows = metrics_counts["valid_rows"]
        dedupe_dropped = metrics_counts["dedupe_dropped"]

        rows_invalid = max(self.metrics["rows_raw"] - valid_rows, 0)
        rows_loaded = max(valid_rows - dedupe_dropped, 0)

        self.metrics["rows_invalid"] = rows_invalid
        self.metrics["dedupe_dropped"] = dedupe_dropped
        self.metrics["rows_loaded"] = rows_loaded

        deduped = ranked.filter(F.col("_row_number") == 1).drop("_row_number", "_ingest_sequence")
        if rows_invalid > 0:
            self.logger.warning("Rejected %s rows with missing transaction_id", rows_invalid)
        if dedupe_dropped > 0:
            self.logger.info("Deduplicated %s rows on transaction_id", dedupe_dropped)

        return deduped

    def _calculate_dedupe_metrics(self, ranked: DataFrame) -> Dict[str, int]:
        """Compute row-level metrics in a single Spark action."""

        metrics_row = ranked.agg(
            F.count(F.lit(1)).alias("valid_rows"),
            F.sum(F.when(F.col("_row_number") > 1, 1).otherwise(0)).alias("dedupe_dropped"),
        ).collect()[0]

        return {
            "valid_rows": int(metrics_row["valid_rows"] or 0),
            "dedupe_dropped": int(metrics_row["dedupe_dropped"] or 0),
        }

    def load(self, df: DataFrame) -> None:
        """Append deduplicated rows to the bronze Delta table."""

        Path(self.bronze_path).parent.mkdir(parents=True, exist_ok=True)
        self.delta_ops.append_with_partitions(df, self.bronze_path, partition_by=["_ingest_date"])
        self.logger.info("Loaded %s rows into %s", self.metrics.get("rows_loaded", 0), self.bronze_path)

    def write_quarantine(self) -> Optional[str]:
        """Persist invalid rows to the quarantine Delta table."""

        if not self._invalid_rows_df or self.metrics.get("rows_invalid", 0) == 0:
            return None

        Path(self.quarantine_path).mkdir(parents=True, exist_ok=True)
        self._invalid_rows_df.write.format("delta").mode("append").save(self.quarantine_path)
        self._quarantine_ready_ts = datetime.utcnow()
        self.logger.warning(
            "Persisted %s invalid rows to quarantine at %s",
            self.metrics.get("rows_invalid", 0),
            self.quarantine_path,
        )
        return self.quarantine_path

    def export_quarantine_records(self) -> Optional[str]:
        """Expose quarantine records for upstream remediation by writing to the configured export path."""

        if not self._invalid_rows_df or self.metrics.get("rows_invalid", 0) == 0:
            self.metrics["quarantine_export_latency_seconds"] = 0.0
            self.metrics["quarantine_ready_within_5_min"] = True
            return None

        Path(self.quarantine_export_path).mkdir(parents=True, exist_ok=True)
        export_target = str(Path(self.quarantine_export_path) / self._run_id)

        export_df = self._invalid_rows_df.dropDuplicates(["_pipeline_run_id", "_source_file", "transaction_id"])
        export_df.write.format("delta").mode("overwrite").save(export_target)

        self._quarantine_ready_ts = datetime.utcnow()
        latency_seconds = (self._quarantine_ready_ts - (self._run_started or self._quarantine_ready_ts)).total_seconds()
        self.metrics["quarantine_export_latency_seconds"] = round(latency_seconds, 2)
        self.metrics["quarantine_ready_within_5_min"] = latency_seconds <= 300
        self.logger.info("Exported quarantine snapshot to %s", export_target)
        return export_target

    def _finalise_metrics(self) -> None:
        if not self._run_started:
            return

        completed_at = datetime.utcnow()
        self.metrics["completed_at"] = completed_at.isoformat()
        duration = (completed_at - self._run_started).total_seconds()
        self.metrics["ingestion_duration_seconds"] = round(duration, 2)
        self.metrics["sla_15_min_passed"] = duration <= 900

    def _log_metrics(self) -> None:
        self.logger.info("Bronze pipeline metrics: %s", json.dumps(self.metrics, default=str))

    def run(self, run_id: str) -> Dict[str, Any]:
        """Execute the bronze ingestion pipeline end-to-end."""

        if not run_id:
            raise ValueError("run_id is required for BronzeTransactionsPipeline")

        self._reset_state(run_id)
        self.logger.info("Starting pipeline: %s", self.__class__.__name__)

        try:
            raw_df = self.extract()
            transformed_df = self.transform(raw_df)
            self.load(transformed_df)
            self.write_quarantine()
            self.export_quarantine_records()
        finally:
            if self._raw_df is not None:
                self._raw_df.unpersist()

        self._finalise_metrics()
        self._log_metrics()

        self.logger.info("Pipeline %s completed successfully", self.__class__.__name__)
        return self.metrics.copy()
