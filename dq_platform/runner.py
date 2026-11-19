"""Run a Great Expectations suite against one table and normalise the results."""
from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import great_expectations as gx
import pandas as pd
from great_expectations.data_context.types.base import ProgressBarsConfig
from sqlalchemy.engine import Engine

from .data import load_keys, load_table
from .suite_builder import build_suite, prepare_frame

SAMPLE_LIMIT = 20


@dataclass
class TableReport:
    table: str
    run_id: str
    rows: int
    duration_seconds: float
    started_at: datetime
    results: list[dict] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def failed(self) -> list[dict]:
        return [r for r in self.results if not r["success"]]

    @property
    def critical_failed(self) -> list[dict]:
        return [r for r in self.failed if r["severity"] == "critical"]

    @property
    def pass_rate(self) -> float:
        return 1.0 if not self.results else (self.total - len(self.failed)) / self.total


def _clean(value):
    """Make values JSON-safe (NaN, numpy scalars, timestamps)."""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if hasattr(value, "item"):
        try:
            return _clean(value.item())
        except Exception:  # pragma: no cover
            pass
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, (list, tuple, set)):
        return [_clean(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _clean(v) for k, v in value.items()}
    return value


def _check_name(etype: str, column, meta: dict) -> str:
    dim = meta.get("dimension")
    if dim in ("referential_integrity", "freshness"):
        return f"{dim}:{meta.get('source_column')}"
    base = etype.removeprefix("expect_")
    if meta.get("check"):
        base = f"schema_{meta['check']}"
    col = column if column else "*"
    if isinstance(col, list):
        col = "+".join(col)
    return f"{base}:{col}"


def normalise(vr, table: str, df: pd.DataFrame, derived: dict, pk: str | None, executed_at: datetime) -> dict:
    cfg = vr.expectation_config
    kwargs = cfg.kwargs
    meta = cfg.meta or {}
    res = vr.result or {}
    column = kwargs.get("column") or kwargs.get("column_list")
    src_col = derived.get(column, column) if not isinstance(column, list) else column

    idx = res.get("unexpected_index_list") or []
    idx = [i for i in idx if i in df.index][:SAMPLE_LIMIT] if idx and not isinstance(idx[0], dict) else []
    bad_ids = _clean(df.loc[idx, pk].tolist()) if idx and pk and pk in df.columns else []
    if isinstance(column, str) and column in derived and idx:
        sample = _clean(df.loc[idx, src_col].tolist())
    else:
        sample = _clean((res.get("partial_unexpected_list") or [])[:SAMPLE_LIMIT])

    if isinstance(observed := res.get("observed_value"), list):  # hide internal helper columns
        res = {**res, "observed_value": [c for c in observed if not str(c).startswith("__")]}

    exc = getattr(vr, "exception_info", None) or {}
    errored = any(v.get("raised_exception") for v in exc.values() if isinstance(v, dict))
    observed = res.get("observed_value")
    if errored:
        msg = next((v.get("exception_message") for v in exc.values() if isinstance(v, dict) and v.get("raised_exception")), "")
        observed = f"ERROR: {msg}"

    return {
        "table_name": table,
        "check_name": _check_name(cfg.type, column, meta),
        "expectation_type": cfg.type,
        "column_name": src_col if not isinstance(src_col, list) else ",".join(src_col),
        "dimension": meta.get("dimension", "other"),
        "severity": meta.get("severity", "critical"),
        "success": bool(vr.success) and not errored,
        "element_count": _clean(res.get("element_count")),
        "unexpected_count": _clean(res.get("unexpected_count")),
        "unexpected_percent": _clean(res.get("unexpected_percent")),
        "observed_value": None if observed is None else str(_clean(observed))[:500],
        "unexpected_sample": sample,
        "bad_row_ids": bad_ids,
        "threshold": meta.get("max_age_hours"),
        "executed_at": executed_at,
    }


def validate_frame(df: pd.DataFrame, table: str, tcfg: dict, parent_keys: dict, run_id: str) -> TableReport:
    """Core validation, independent of where the DataFrame came from (easy to unit test)."""
    started = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    df, derived = prepare_frame(df.copy(), tcfg, parent_keys)

    context = gx.get_context(mode="ephemeral")
    context.variables.progress_bars = ProgressBarsConfig(globally=False, metric_calculations=False)
    asset = context.data_sources.add_pandas(f"src_{table}").add_dataframe_asset(table)
    batch = asset.add_batch_definition_whole_dataframe("whole").get_batch(batch_parameters={"dataframe": df})
    suite = build_suite(table, tcfg)
    out = batch.validate(suite, result_format="COMPLETE")

    pk = tcfg.get("primary_key")
    results = [normalise(vr, table, df, derived, pk, started) for vr in out.results]
    return TableReport(table, run_id, int(len(df)), time.perf_counter() - t0, started, results)


def run_table(engine: Engine, cfg: dict, table: str, run_id: str) -> TableReport:
    tcfg = cfg["tables"][table]
    schema = tcfg.get("schema")
    ts_cols = [c for c, t in (tcfg.get("schema_check", {}).get("columns", {})).items() if t == "timestamp"]
    df = load_table(engine, schema, table, ts_cols)

    parent_keys = {}
    for c in tcfg.get("checks", []):
        if c["type"] == "referential":
            ref = cfg["tables"][c["ref_table"]]
            parent_keys[(c["ref_table"], c["ref_column"])] = load_keys(
                engine, ref.get("schema"), c["ref_table"], c["ref_column"])
    return validate_frame(df, table, tcfg, parent_keys, run_id)
