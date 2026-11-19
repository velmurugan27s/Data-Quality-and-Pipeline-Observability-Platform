"""Publish validation metrics to the Prometheus Pushgateway.

Airflow tasks are short-lived, so they cannot be scraped; they push instead.
The `table` label comes from the Pushgateway grouping key (so it is not a metric label here).
"""
from __future__ import annotations

import time
from collections import Counter

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from .runner import TableReport

JOB = "dq_validation"
PIPELINE_JOB = "dq_pipeline"


def push_table_metrics(report: TableReport, gateway: str) -> None:
    reg = CollectorRegistry()
    Gauge("dq_table_rows", "Rows scanned", registry=reg).set(report.rows)
    Gauge("dq_table_checks_total", "Checks executed", registry=reg).set(report.total)
    Gauge("dq_table_failed_checks", "Failed checks", registry=reg).set(len(report.failed))
    Gauge("dq_table_critical_failed_checks", "Failed critical checks", registry=reg).set(len(report.critical_failed))
    Gauge("dq_table_pass_rate", "Fraction of checks passing (0-1)", registry=reg).set(report.pass_rate)
    Gauge("dq_table_status", "1 = no critical failures, 0 = critical failure", registry=reg).set(0 if report.critical_failed else 1)
    Gauge("dq_validation_duration_seconds", "Validation wall time", registry=reg).set(report.duration_seconds)
    Gauge("dq_validation_last_run_timestamp_seconds", "Unix time of last validation", registry=reg).set(time.time())

    by_dim = Gauge("dq_failed_checks", "Failed checks by dimension/severity", ["dimension", "severity"], registry=reg)
    counts = Counter((r["dimension"], r["severity"]) for r in report.failed)
    for dim, sev in {(r["dimension"], r["severity"]) for r in report.results}:
        by_dim.labels(dim, sev).set(counts.get((dim, sev), 0))

    ok = Gauge("dq_check_success", "1 = check passed", ["check", "dimension", "severity"], registry=reg)
    pct = Gauge("dq_check_unexpected_percent", "Percent of rows violating the check", ["check", "dimension"], registry=reg)
    for r in report.results:
        ok.labels(r["check_name"], r["dimension"], r["severity"]).set(1 if r["success"] else 0)
        if r["unexpected_percent"] is not None:
            pct.labels(r["check_name"], r["dimension"]).set(r["unexpected_percent"])

    age = Gauge("dq_freshness_age_hours", "Age of newest record", ["column"], registry=reg)
    thr = Gauge("dq_freshness_threshold_hours", "Allowed age of newest record", ["column"], registry=reg)
    for r in report.results:
        if r["dimension"] == "freshness" and r["observed_value"] not in (None, "None") and not str(r["observed_value"]).startswith("ERROR"):
            age.labels(r["column_name"]).set(float(r["observed_value"]))
            thr.labels(r["column_name"]).set(float(r["threshold"]))

    push_to_gateway(gateway, job=JOB, registry=reg, grouping_key={"table": report.table})


def push_pipeline_metrics(summary: dict, gateway: str, pipeline: str = "dq_pipeline") -> None:
    reg = CollectorRegistry()
    Gauge("dq_pipeline_pass_rate", "Pass rate of last run", registry=reg).set(summary["pass_rate"])
    Gauge("dq_pipeline_failed_checks", "Failed checks in last run", registry=reg).set(summary["failed_checks"])
    Gauge("dq_pipeline_critical_failed_checks", "Critical failures in last run", registry=reg).set(summary["critical_failed"])
    Gauge("dq_pipeline_last_run_timestamp_seconds", "Unix time of last summary", registry=reg).set(time.time())
    push_to_gateway(gateway, job=PIPELINE_JOB, registry=reg, grouping_key={"pipeline": pipeline})
