"""Runtime settings (all overridable through environment variables)."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")  # no telemetry from pipelines
os.environ.setdefault("TQDM_DISABLE", "1")  # no progress bars in task logs
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_CONFIG = Path(os.getenv("DQ_CONFIG", Path(__file__).resolve().parent.parent / "config" / "expectations.yaml"))


@dataclass(frozen=True)
class Settings:
    warehouse_url: str
    results_schema: str | None
    pushgateway: str | None

    @staticmethod
    def load() -> "Settings":
        return Settings(
            warehouse_url=os.getenv(
                "DQ_WAREHOUSE_URL",
                "postgresql+psycopg2://warehouse:warehouse@localhost:5432/warehouse",
            ),
            results_schema=os.getenv("DQ_RESULTS_SCHEMA", "dq") or None,
            pushgateway=os.getenv("DQ_PUSHGATEWAY", "") or None,  # e.g. "pushgateway:9091"
        )


def safe_ident(name: str) -> str:
    """Validate SQL identifiers coming from YAML before interpolating them."""
    if not _IDENT.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def load_config(path: str | Path | None = None) -> dict:
    with open(path or DEFAULT_CONFIG, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    cfg.setdefault("defaults", {})
    for tname, tcfg in cfg["tables"].items():
        safe_ident(tname)
        tcfg.setdefault("schema", cfg["defaults"].get("schema"))
    return cfg
