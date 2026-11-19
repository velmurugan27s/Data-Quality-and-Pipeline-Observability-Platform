"""Runs the full pipeline against a real PostgreSQL.

    DQ_TEST_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/warehouse pytest tests/test_postgres_integration.py
"""
import os
import subprocess
import sys

import pytest
from sqlalchemy import text

from dq_platform.config import load_config
from dq_platform.data import get_engine
from dq_platform.runner import run_table
from dq_platform.store import init_store, save_report, summarise_run

URL = os.getenv("DQ_TEST_URL")
pytestmark = pytest.mark.skipif(not URL, reason="set DQ_TEST_URL to run against PostgreSQL")


def seed(scenario):
    subprocess.run([sys.executable, "-m", "scripts.seed_data", "--scenario", scenario, "--orders", "300"],
                   check=True, env={**os.environ, "DQ_WAREHOUSE_URL": URL}, capture_output=True)


def run_all(run_id):
    cfg, engine = load_config(), get_engine(URL)
    init_store(engine, "dq")
    for t in cfg["tables"]:
        save_report(engine, "dq", run_table(engine, cfg, t, run_id))
    return summarise_run(engine, "dq", run_id), engine


def test_clean_scenario_passes():
    seed("clean")
    s, _ = run_all("it_clean")
    assert s["status"] == "passed" and s["pass_rate"] == 1.0


def test_dirty_scenario_catches_every_dimension():
    seed("dirty")
    s, engine = run_all("it_dirty")
    assert s["status"] == "failed"
    with engine.connect() as c:
        dims = {r[0] for r in c.execute(text(
            "select distinct dimension from dq.dq_check_results where run_id='it_dirty' and not success"))}
    assert {"uniqueness", "completeness", "validity", "referential_integrity"} <= dims


def test_stale_and_drift():
    seed("stale")
    _, engine = run_all("it_stale")
    with engine.connect() as c:
        assert c.execute(text("select count(*) from dq.dq_check_results where run_id='it_stale' and dimension='freshness' and not success")).scalar() >= 1
    seed("drift")
    run_all("it_drift")
    with engine.connect() as c:
        assert c.execute(text("select count(*) from dq.dq_check_results where run_id='it_drift' and dimension='schema' and not success")).scalar() >= 2
