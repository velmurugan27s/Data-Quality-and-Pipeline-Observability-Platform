from datetime import datetime, timezone
from unittest import mock

import pandas as pd
from sqlalchemy import create_engine, text

from dq_platform import metrics
from dq_platform.config import load_config
from dq_platform.runner import validate_frame
from dq_platform.store import init_store, save_report, summarise_run

CFG = load_config()
NOW = pd.Timestamp.now(tz="UTC").tz_localize(None)


def bad_report(run_id="r1"):
    df = pd.DataFrame({
        "customer_id": [1, 1, 3], "email": ["a@x.com", "b@x.com", None], "full_name": list("abc"),
        "country": ["IN"] * 3, "created_at": [NOW] * 3, "updated_at": [NOW - pd.Timedelta(days=9)] * 3})
    return validate_frame(df, "customers", CFG["tables"]["customers"], {}, run_id)


def test_store_roundtrip_and_gate():
    engine = create_engine("sqlite://")
    init_store(engine, None)
    save_report(engine, None, bad_report())
    save_report(engine, None, bad_report())  # idempotent (Airflow retry)
    with engine.connect() as c:
        n = c.execute(text("select count(*) from dq_check_results")).scalar()
    assert n == bad_report().total
    s = summarise_run(engine, None, "r1")
    assert s["status"] == "failed" and s["critical_failed"] >= 3 and 0 < s["pass_rate"] < 1
    assert summarise_run(engine, None, "nope")["status"] == "passed"  # no data -> nothing failed


def test_metrics_pushed_per_table():
    rep = bad_report()
    with mock.patch.object(metrics, "push_to_gateway") as push:
        metrics.push_table_metrics(rep, "pg:9091")
    kw = push.call_args.kwargs
    assert kw["job"] == "dq_validation" and kw["grouping_key"] == {"table": "customers"}
    text_ = "".join(m.name + " " for m in kw["registry"].collect())
    for name in ("dq_table_pass_rate", "dq_failed_checks", "dq_check_success", "dq_freshness_age_hours",
                 "dq_validation_duration_seconds"):
        assert name in text_
    vals = {m.name: m.samples[0].value for m in kw["registry"].collect() if m.name in ("dq_table_status",)}
    assert vals["dq_table_status"] == 0
