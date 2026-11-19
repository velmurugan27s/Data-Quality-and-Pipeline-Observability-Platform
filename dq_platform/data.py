"""Data access: load tables from the warehouse into DataFrames."""
from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import safe_ident


def get_engine(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True)


def qualified(schema: str | None, table: str) -> str:
    table = safe_ident(table)
    return f'"{safe_ident(schema)}"."{table}"' if schema else f'"{table}"'


def load_table(engine: Engine, schema: str | None, table: str, ts_columns=()) -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql(text(f"SELECT * FROM {qualified(schema, table)}"), conn, coerce_float=True)
    # Normalise real datetime columns to naive UTC. Columns that are NOT datetimes are left
    # untouched on purpose, so type drift (e.g. a timestamp turning into text) is caught by the schema check.
    for col in ts_columns:
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            s = pd.to_datetime(df[col], utc=True)
            df[col] = s.dt.tz_convert("UTC").dt.tz_localize(None)
    return df


def load_keys(engine: Engine, schema: str | None, table: str, column: str) -> set:
    sql = f"SELECT DISTINCT {safe_ident(column)} FROM {qualified(schema, table)} WHERE {safe_ident(column)} IS NOT NULL"
    with engine.connect() as conn:
        return {r[0] for r in conn.execute(text(sql))}
