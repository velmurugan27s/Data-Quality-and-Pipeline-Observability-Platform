"""Persist validation results to PostgreSQL (schema `dq`) for history, Grafana tables and the quality gate."""
from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import (BigInteger, Boolean, Column, DateTime, Float, Integer, MetaData, String, Table, Text,
                        func, select)
from sqlalchemy.engine import Engine

from .runner import TableReport


def _tables(schema: str | None):
    md = MetaData(schema=schema)
    results = Table(
        "dq_check_results", md,
        Column("id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True),
        Column("run_id", String(200), index=True, nullable=False),
        Column("table_name", String(100), nullable=False),
        Column("check_name", String(300), nullable=False),
        Column("expectation_type", String(120)),
        Column("column_name", String(200)),
        Column("dimension", String(50)),
        Column("severity", String(20)),
        Column("success", Boolean, nullable=False),
        Column("element_count", Float),
        Column("unexpected_count", Float),
        Column("unexpected_percent", Float),
        Column("observed_value", Text),
        Column("unexpected_sample", Text),   # JSON list of offending values
        Column("bad_row_ids", Text),         # JSON list of primary keys of offending rows
        Column("executed_at", DateTime(timezone=True), nullable=False),
    )
    summary = Table(
        "dq_run_summary", md,
        Column("run_id", String(200), primary_key=True),
        Column("finished_at", DateTime(timezone=True), nullable=False),
        Column("tables_validated", Integer),
        Column("total_checks", Integer),
        Column("failed_checks", Integer),
        Column("critical_failed", Integer),
        Column("pass_rate", Float),
        Column("status", String(20)),
    )
    return md, results, summary


def init_store(engine: Engine, schema: str | None):
    md, _, _ = _tables(schema)
    with engine.begin() as conn:
        if schema and engine.dialect.name == "postgresql":
            conn.exec_driver_sql(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        md.create_all(conn, checkfirst=True)


def save_report(engine: Engine, schema: str | None, report: TableReport):
    _, results, _ = _tables(schema)
    rows = []
    for r in report.results:
        rows.append({
            "run_id": report.run_id, "table_name": r["table_name"], "check_name": r["check_name"],
            "expectation_type": r["expectation_type"], "column_name": r["column_name"],
            "dimension": r["dimension"], "severity": r["severity"], "success": r["success"],
            "element_count": r["element_count"], "unexpected_count": r["unexpected_count"],
            "unexpected_percent": r["unexpected_percent"], "observed_value": r["observed_value"],
            "unexpected_sample": json.dumps(r["unexpected_sample"], default=str),
            "bad_row_ids": json.dumps(r["bad_row_ids"], default=str),
            "executed_at": r["executed_at"],
        })
    with engine.begin() as conn:
        # idempotent for Airflow retries: replace this table's results for the run
        conn.execute(results.delete().where((results.c.run_id == report.run_id) & (results.c.table_name == report.table)))
        if rows:
            conn.execute(results.insert(), rows)


def summarise_run(engine: Engine, schema: str | None, run_id: str) -> dict:
    _, results, summary = _tables(schema)
    with engine.begin() as conn:
        q = select(
            func.count(), 
            func.sum(func.cast(~results.c.success, Integer)),
            func.sum(func.cast((~results.c.success) & (results.c.severity == "critical"), Integer)),
            func.count(func.distinct(results.c.table_name)),
        ).where(results.c.run_id == run_id)
        total, failed, crit, tables = conn.execute(q).one()
        total, failed, crit = int(total or 0), int(failed or 0), int(crit or 0)
        out = {
            "run_id": run_id, "tables_validated": int(tables or 0), "total_checks": total,
            "failed_checks": failed, "critical_failed": crit,
            "pass_rate": 1.0 if total == 0 else (total - failed) / total,
            "status": "failed" if crit else ("warning" if failed else "passed"),
            "finished_at": datetime.now(timezone.utc),
        }
        conn.execute(summary.delete().where(summary.c.run_id == run_id))
        conn.execute(summary.insert(), out)
    return out
