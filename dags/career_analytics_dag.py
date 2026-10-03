"""Airflow 2.10.1 DAG for the career analytics pipeline."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context

from career_analytics.etl import run_pipeline


SOURCE_PATH = Path(
    os.environ.get(
        "CAREER_ANALYTICS_SOURCE",
        "/opt/airflow/data/incoming/full-professionals-json.json",
    )
)
TARGET_PATH = Path(
    os.environ.get(
        "CAREER_ANALYTICS_DB",
        "/opt/airflow/data/output/career_analytics.duckdb",
    )
)


@dag(
    dag_id="career_analytics_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # DuckDB supports one writer at a time.
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    },
    tags=["career-analytics", "futurefit"],
)
def career_analytics_pipeline():
    @task
    def inspect_source() -> dict[str, str]:
        context = get_current_context()
        if not SOURCE_PATH.exists():
            raise AirflowFailException(f"Source file not found: {SOURCE_PATH}")
        return {
            "source_path": str(SOURCE_PATH),
            "target_path": str(TARGET_PATH),
            "as_of_date": context["data_interval_end"].date().isoformat(),
        }

    @task
    def load_validated_snapshot(config: dict[str, str]) -> dict[str, str | int]:
        summary = run_pipeline(
            source_path=Path(config["source_path"]),
            target_path=Path(config["target_path"]),
            mode="incremental",
            as_of_date=datetime.fromisoformat(config["as_of_date"]).date(),
            strict=True,
        )
        return {
            "target_path": config["target_path"],
            "source_professional_count": summary["source_counts"]["professionals"],
            "changed_professionals": summary["changed_professionals"],
        }

    @task
    def reconcile(result: dict[str, str | int]) -> None:
        with duckdb.connect(str(result["target_path"]), read_only=True) as connection:
            target_count, metric_count, errors = connection.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM dim_professional),
                    (SELECT COUNT(*) FROM mart_professional_career_metrics),
                    (SELECT COUNT(*) FROM data_quality_issues WHERE severity = 'error')
                """
            ).fetchone()
        source_count = result["source_professional_count"]
        if source_count != target_count or target_count != metric_count or errors:
            raise AirflowFailException(
                "Reconciliation failed: "
                f"source={source_count}, target={target_count}, metrics={metric_count}, errors={errors}"
            )

    reconcile(load_validated_snapshot(inspect_source()))


career_analytics_pipeline()
