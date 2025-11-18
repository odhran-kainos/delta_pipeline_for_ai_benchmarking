import json
import logging
from pathlib import Path
from typing import Any, Dict
from uuid import uuid4

from prefect import flow, task

from pipelines.bronze_transactions_pipeline import BronzeTransactionsPipeline
from pipelines.utils.spark_session import create_spark_session


@task
def run_bronze_ingestion(config_path: str = "config/pipeline_config.yaml") -> Dict[str, Any]:
    """Execute the bronze transactions pipeline with a Prefect-supplied run identifier."""

    spark = create_spark_session()
    try:
        pipeline = BronzeTransactionsPipeline(spark, config_path=config_path)
        run_id = f"bronze-prefect-{uuid4()}"
        metrics = pipeline.run(run_id=run_id)
        logging.info("Bronze ingestion metrics: %s", json.dumps(metrics))

        export_root = Path(metrics["config_snapshot"]["quarantine"]["export_path"])
        logging.info("Quarantine export available at %s/%s", export_root, metrics["pipeline_run_id"])
        return metrics
    finally:
        spark.stop()


@task
def validate_bronze_metrics(metrics: Dict[str, Any]) -> str:
    """Validate SLA compliance and metric reconciliation for bronze ingestion."""

    if metrics["rows_loaded"] + metrics["rows_invalid"] != metrics["rows_raw"]:
        raise ValueError("Metric reconciliation failed for bronze ingestion")

    if not metrics.get("sla_15_min_passed", False):
        raise ValueError("Bronze ingestion exceeded the 15 minute SLA")

    logging.info(
        "Operator handoff: review export dataset at %s/%s",
        metrics["config_snapshot"]["quarantine"]["export_path"],
        metrics["pipeline_run_id"],
    )
    return "Bronze ingestion validated"


@flow(name="bronze-transactions-ingestion")
def bronze_ingestion_flow(config_path: str = "config/pipeline_config.yaml") -> Dict[str, Any]:
    """Prefect flow that runs bronze ingestion and surfaces metrics for operators."""

    logging.info("Starting bronze transactions ingestion flow")
    metrics = run_bronze_ingestion(config_path)
    validation_status = validate_bronze_metrics(metrics)
    logging.info("Bronze ingestion flow finished")
    return {"metrics": metrics, "validation": validation_status}


if __name__ == "__main__":
    bronze_ingestion_flow()