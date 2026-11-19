"""Data-quality validation DAG.

   start -> validate_<table> (parallel, one task per table) -> summarize (quality gate) -> publish_certified_data

* Each validate task runs a Great Expectations suite, stores results in Postgres and pushes metrics to Prometheus.
* A *critical* failure fails the task with AirflowFailException (no pointless retries); infrastructure errors retry.
* `summarize` runs even when validations failed (trigger_rule=all_done), publishes pipeline-level metrics and
  fails the DAG run if the run had any critical failure -> `publish_certified_data` is skipped ("upstream_failed").
"""
from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta

import yaml
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)
DQ_HOME = os.getenv("DQ_HOME", "/opt/dq")
DQ_PYTHON = os.getenv("DQ_PYTHON", "/opt/dq-venv/bin/python")
CONFIG = os.getenv("DQ_CONFIG", f"{DQ_HOME}/config/expectations.yaml")


def _tables() -> list[str]:
    with open(CONFIG, "r", encoding="utf-8") as fh:
        return list(yaml.safe_load(fh)["tables"])


def run_dq(args: list[str]) -> None:
    """Run the DQ CLI in its isolated venv. Exit code: 0 ok | 1 data-quality failure | other = infra error."""
    proc = subprocess.run(
        [DQ_PYTHON, "-m", "dq_platform.cli", *args],
        cwd=DQ_HOME, env={**os.environ, "PYTHONPATH": DQ_HOME}, capture_output=True, text=True,
    )
    print(proc.stdout)
    if proc.stderr:
        print(proc.stderr)
    if proc.returncode == 1:
        raise AirflowFailException("Critical data-quality check(s) failed - see task log")
    if proc.returncode != 0:
        raise AirflowException(f"DQ CLI infrastructure error (exit {proc.returncode})")


def notify_failure(context) -> None:
    ti = context["task_instance"]
    log.error("DQ ALERT: task %s.%s failed (run %s)", ti.dag_id, ti.task_id, context["run_id"])


default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="dq_pipeline",
    description="Great Expectations validation of the retail warehouse",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "great-expectations"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")

    validations = [
        PythonOperator(
            task_id=f"validate_{table}",
            python_callable=run_dq,
            op_args=[["run", "--table", table, "--run-id", "{{ run_id }}"]],
        )
        for table in _tables()
    ]

    summarize = PythonOperator(
        task_id="summarize",
        python_callable=run_dq,
        op_args=[["summarize", "--run-id", "{{ run_id }}"]],
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )

    publish = EmptyOperator(task_id="publish_certified_data")  # replace with the real downstream load/promotion

    start >> validations >> summarize >> publish
