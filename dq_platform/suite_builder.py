"""Translate the declarative YAML config into Great Expectations expectations.

Dimensions covered
    completeness          not_null
    uniqueness            unique / unique_together
    schema                schema_check (column set + types)
    referential_integrity referential
    freshness             freshness
    validity              regex / in_set / between
    volume                row_count
"""
from __future__ import annotations

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

TYPE_MAP = {
    "integer": ["int64", "Int64", "int32", "Int32"],
    "number": ["float64", "Float64", "int64", "Int64", "float32"],
    "string": ["object", "str", "string"],
    "boolean": ["bool", "boolean", "object"],
    "timestamp": ["datetime64[ns]", "datetime64[us]", "datetime64[ms]"],  # loader normalises to naive UTC
}


def _meta(check: dict, dimension: str) -> dict:
    return {
        "dimension": dimension,
        "severity": check.get("severity", "critical"),
        "description": check.get("description", ""),
    }


def _mostly(check: dict) -> dict:
    return {"mostly": check["mostly"]} if "mostly" in check else {}


def prepare_frame(df: pd.DataFrame, tcfg: dict, parent_keys: dict) -> tuple[pd.DataFrame, dict]:
    """Add derived helper columns GX can assert on. Returns (frame, derived_map).

    derived_map: helper column -> source column (used to report offending values).
    """
    derived: dict[str, str] = {}
    now = pd.Timestamp.now(tz="UTC")
    for check in tcfg.get("checks", []):
        if check["type"] == "freshness":
            src, helper = check["column"], f"__age_hours__{check['column']}"
            if src in df.columns:
                df[helper] = (now - pd.to_datetime(df[src], utc=True, errors="coerce")).dt.total_seconds() / 3600.0
                derived[helper] = src
        elif check["type"] == "referential":
            src = check["column"]
            helper = f"__ref__{src}__{check['ref_table']}"
            if src in df.columns:
                keys = parent_keys[(check["ref_table"], check["ref_column"])]
                df[helper] = df[src].isna() | df[src].isin(keys)  # nulls are covered by not_null
                derived[helper] = src
    return df, derived


def build_suite(table: str, tcfg: dict) -> gx.ExpectationSuite:
    suite = gx.ExpectationSuite(name=f"{table}_suite")

    def add(exp):
        suite.add_expectation(exp)

    sch = tcfg.get("schema_check")
    if sch:
        cols = sch["columns"]
        # helper columns added by prepare_frame() are internal, so allow them in the exact match
        helpers = [f"__age_hours__{c['column']}" for c in tcfg.get("checks", []) if c["type"] == "freshness"]
        helpers += [f"__ref__{c['column']}__{c['ref_table']}" for c in tcfg.get("checks", []) if c["type"] == "referential"]
        add(gxe.ExpectTableColumnsToMatchSet(
            column_set=list(cols) + helpers, exact_match=sch.get("exact", True),
            meta=_meta(sch, "schema") | {"check": "columns"}))
        for col, typ in cols.items():
            add(gxe.ExpectColumnValuesToBeInTypeList(
                column=col, type_list=TYPE_MAP[typ],
                meta=_meta(sch, "schema") | {"check": f"type[{typ}]"}))

    for c in tcfg.get("checks", []):
        t = c["type"]
        if t == "not_null":
            add(gxe.ExpectColumnValuesToNotBeNull(column=c["column"], **_mostly(c), meta=_meta(c, "completeness")))
        elif t == "unique":
            add(gxe.ExpectColumnValuesToBeUnique(column=c["column"], **_mostly(c), meta=_meta(c, "uniqueness")))
        elif t == "unique_together":
            add(gxe.ExpectCompoundColumnsToBeUnique(column_list=c["columns"], **_mostly(c), meta=_meta(c, "uniqueness")))
        elif t == "regex":
            add(gxe.ExpectColumnValuesToMatchRegex(column=c["column"], regex=c["regex"], **_mostly(c), meta=_meta(c, "validity")))
        elif t == "in_set":
            add(gxe.ExpectColumnValuesToBeInSet(column=c["column"], value_set=c["values"], **_mostly(c), meta=_meta(c, "validity")))
        elif t == "between":
            add(gxe.ExpectColumnValuesToBeBetween(
                column=c["column"], min_value=c.get("min"), max_value=c.get("max"),
                **_mostly(c), meta=_meta(c, "validity")))
        elif t == "row_count":
            add(gxe.ExpectTableRowCountToBeBetween(min_value=c.get("min"), max_value=c.get("max"), meta=_meta(c, "volume")))
        elif t == "referential":
            helper = f"__ref__{c['column']}__{c['ref_table']}"
            add(gxe.ExpectColumnValuesToBeInSet(
                column=helper, value_set=[True], **_mostly(c),
                meta=_meta(c, "referential_integrity") | {
                    "source_column": c["column"], "ref": f"{c['ref_table']}.{c['ref_column']}"}))
        elif t == "freshness":
            helper = f"__age_hours__{c['column']}"
            add(gxe.ExpectColumnMinToBeBetween(
                column=helper, min_value=None, max_value=float(c["max_age_hours"]),
                meta=_meta(c, "freshness") | {
                    "source_column": c["column"], "max_age_hours": float(c["max_age_hours"])}))
        else:
            raise ValueError(f"Unknown check type: {t}")
    return suite
