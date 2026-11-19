import pandas as pd
import pytest

from dq_platform.config import load_config
from dq_platform.runner import validate_frame

CFG = load_config()
NOW = pd.Timestamp.now(tz="UTC").tz_localize(None)


def customers(**over):
    df = pd.DataFrame({
        "customer_id": [1, 2, 3, 4], "email": ["a@x.com", "b@x.com", "c@x.com", "d@x.com"],
        "full_name": list("abcd"), "country": ["IN"] * 4,
        "created_at": [NOW] * 4, "updated_at": [NOW] * 4,
    })
    for k, v in over.items():
        df[k] = v
    return df


def run(df, table="customers", parent_keys=None):
    return validate_frame(df, table, CFG["tables"][table], parent_keys or {}, "t")


def failed(rep, dimension):
    return [r for r in rep.failed if r["dimension"] == dimension]


def test_clean_data_passes():
    rep = run(customers())
    assert rep.failed == [] and rep.pass_rate == 1.0


def test_duplicates_detected_with_row_ids():
    rep = run(customers(customer_id=[1, 2, 2, 4]))
    f = failed(rep, "uniqueness")
    assert f and f[0]["unexpected_count"] == 2 and f[0]["severity"] == "critical"
    assert set(f[0]["bad_row_ids"]) == {2}


def test_missing_values_detected():
    rep = run(customers(email=["a@x.com", None, "c@x.com", "d@x.com"]))
    f = [r for r in failed(rep, "completeness") if r["column_name"] == "email"]
    assert f and f[0]["unexpected_count"] == 1 and f[0]["bad_row_ids"] == [2]


def test_mostly_threshold_tolerates_small_nulls():
    df = pd.concat([customers()] * 1).reset_index(drop=True)
    big = pd.concat([df] * 50, ignore_index=True)
    big["customer_id"] = range(len(big))
    big["email"] = [f"u{i}@x.com" for i in range(len(big))]
    big.loc[0, "full_name"] = None  # 1/200 nulls, mostly=0.98 -> OK
    assert not [r for r in run(big).failed if r["column_name"] == "full_name"]


def test_schema_drift_new_column_and_type_change():
    df = customers()
    df["loyalty_tier"] = "gold"
    assert any(r["check_name"].startswith("schema_columns") for r in failed(run(df), "schema"))
    df2 = customers(customer_id=["1", "2", "3", "4"])
    assert any("customer_id" == r["column_name"] for r in failed(run(df2), "schema"))


def test_missing_column_detected():
    rep = run(customers().drop(columns=["country"]))
    assert failed(rep, "schema")


def test_freshness_breach():
    rep = run(customers(updated_at=[NOW - pd.Timedelta(days=10)] * 4))
    f = failed(rep, "freshness")
    assert f and float(f[0]["observed_value"]) > 200 and f[0]["threshold"] == 48.0


def test_referential_integrity_orphans():
    orders = pd.DataFrame({
        "order_id": [1, 2, 3], "customer_id": [1, 2, 99], "status": ["paid"] * 3,
        "order_total": [10.0, 20.0, 30.0], "order_ts": [NOW] * 3, "updated_at": [NOW] * 3})
    rep = run(orders, "orders", {("customers", "customer_id"): {1, 2, 3}})
    f = failed(rep, "referential_integrity")
    assert len(f) == 1 and f[0]["unexpected_count"] == 1
    assert f[0]["bad_row_ids"] == [3] and f[0]["unexpected_sample"] == [99]


def test_validity_rules():
    orders = pd.DataFrame({
        "order_id": [1, 2], "customer_id": [1, 1], "status": ["paid", "teleported"],
        "order_total": [10.0, -5.0], "order_ts": [NOW] * 2, "updated_at": [NOW] * 2})
    rep = run(orders, "orders", {("customers", "customer_id"): {1}})
    names = {r["check_name"] for r in failed(rep, "validity")}
    assert names == {"column_values_to_be_in_set:status", "column_values_to_be_between:order_total"}


def test_empty_table_fails_volume_check():
    rep = run(customers().iloc[0:0])
    assert failed(rep, "volume")


def test_critical_vs_warning_split():
    rep = run(customers(email=["a@x.com", "bad", "c@x.com", "d@x.com"]))  # regex rule is a warning
    assert rep.failed and not rep.critical_failed
