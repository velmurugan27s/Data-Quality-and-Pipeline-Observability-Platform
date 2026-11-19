"""Manually triggered DAG that (re)seeds the demo warehouse with a chosen scenario.

Trigger with config, e.g. {"scenario": "dirty"} - or pick from the dropdown in the Airflow UI.
"""
from __future__ import annotations

import os
import subprocess
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

DQ_HOME = os.getenv("DQ_HOME", "/opt/dq")
DQ_PYTHON = os.getenv("DQ_PYTHON", "/opt/dq-venv/bin/python")


def seed(scenario: str) -> None:
    proc = subprocess.run([DQ_PYTHON, "-m", "scripts.seed_data", "--scenario", scenario], cwd=DQ_HOME,
                          env={**os.environ, "PYTHONPATH": DQ_HOME}, capture_output=True, text=True)
    print(proc.stdout, proc.stderr)
    if proc.returncode:
        raise AirflowException("seeding failed")


with DAG(
    dag_id="dq_load_demo_data",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"scenario": Param("dirty", enum=["clean", "dirty", "stale", "drift"],
                              description="clean = all pass | dirty = bad records | stale = freshness breach | drift = schema break")},
    tags=["data-quality", "demo"],
) as dag:
    PythonOperator(task_id="seed_warehouse", python_callable=seed, op_args=["{{ params.scenario }}"])
