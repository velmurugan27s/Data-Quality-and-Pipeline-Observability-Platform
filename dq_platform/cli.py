"""Command line entry point (used by Airflow tasks and for local runs).

    python -m dq_platform.cli run --table all
    python -m dq_platform.cli run --table orders --run-id manual_1
    python -m dq_platform.cli summarize --run-id manual_1

Exit codes: 0 = ok (warnings allowed) | 1 = critical data-quality failure | 2 = infrastructure error
"""
from __future__ import annotations

import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone

from .config import Settings, load_config
from .data import get_engine
from .metrics import push_pipeline_metrics, push_table_metrics
from .runner import TableReport, run_table
from .store import init_store, save_report, summarise_run

log = logging.getLogger("dq")


def _print_report(rep: TableReport) -> None:
    print(f"\n=== {rep.table}: {rep.total - len(rep.failed)}/{rep.total} checks passed "
          f"({rep.pass_rate:.0%}), {rep.rows} rows, {rep.duration_seconds:.2f}s ===")
    for r in rep.failed:
        extra = f" unexpected={int(r['unexpected_count'])}" if r["unexpected_count"] is not None else ""
        extra += f" observed={r['observed_value']}" if r["observed_value"] and r["unexpected_count"] is None else ""
        ids = f" bad_ids={r['bad_row_ids'][:5]}" if r["bad_row_ids"] else ""
        print(f"  [{r['severity'].upper():8}] {r['dimension']:22} {r['check_name']}{extra}{ids}")


def cmd_run(args) -> int:
    st = Settings.load()
    cfg = load_config(args.config)
    engine = get_engine(st.warehouse_url)
    init_store(engine, st.results_schema)
    run_id = args.run_id or f"manual__{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"
    tables = list(cfg["tables"]) if args.table == "all" else [args.table]
    critical = 0
    for t in tables:
        if t not in cfg["tables"]:
            print(f"Unknown table {t!r}. Known: {', '.join(cfg['tables'])}", file=sys.stderr)
            return 2
        rep = run_table(engine, cfg, t, run_id)
        save_report(engine, st.results_schema, rep)
        if st.pushgateway and not args.no_push:
            try:
                push_table_metrics(rep, st.pushgateway)
            except Exception as exc:  # monitoring must never mask the DQ result
                log.warning("Could not push metrics: %s", exc)
        _print_report(rep)
        critical += len(rep.critical_failed)
    print(f"\nrun_id={run_id} critical_failures={critical}")
    return 1 if critical else 0


def cmd_summarize(args) -> int:
    st = Settings.load()
    engine = get_engine(st.warehouse_url)
    init_store(engine, st.results_schema)
    s = summarise_run(engine, st.results_schema, args.run_id)
    if st.pushgateway and not args.no_push:
        try:
            push_pipeline_metrics(s, st.pushgateway)
        except Exception as exc:
            log.warning("Could not push metrics: %s", exc)
    print(f"run {s['run_id']}: status={s['status']} tables={s['tables_validated']} checks={s['total_checks']} "
          f"failed={s['failed_checks']} critical={s['critical_failed']} pass_rate={s['pass_rate']:.1%}")
    return 1 if s["critical_failed"] else 0


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("great_expectations").setLevel(logging.WARNING)
    p = argparse.ArgumentParser(prog="dq")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="validate one or all tables")
    r.add_argument("--table", default="all")
    r.add_argument("--run-id")
    r.add_argument("--config")
    r.add_argument("--no-push", action="store_true")
    r.set_defaults(fn=cmd_run)
    s = sub.add_parser("summarize", help="aggregate a run, push pipeline metrics, apply the quality gate")
    s.add_argument("--run-id", required=True)
    s.add_argument("--no-push", action="store_true")
    s.set_defaults(fn=cmd_summarize)
    args = p.parse_args(argv)
    try:
        return args.fn(args)
    except Exception as exc:
        log.exception("Infrastructure error: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
