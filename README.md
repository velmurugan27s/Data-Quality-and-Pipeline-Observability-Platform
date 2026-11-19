# Data Quality & Pipeline Observability Platform

**Airflow · Great Expectations · PostgreSQL · Prometheus · Grafana · Alertmanager**

An automated platform that validates warehouse tables on a schedule, stores every result, blocks bad data
from being published, and exposes pipeline health and data-quality metrics with alerting.

```
 PostgreSQL (retail raw layer)
        │  read
        ▼
 Airflow DAG  dq_pipeline ──► Great Expectations suites (one per table, built from config/expectations.yaml)
   validate_* ─► summarize (quality gate) ─► publish_certified_data
        │ results                │ metrics                      │ task/DAG stats
        ▼                        ▼                              ▼
 PostgreSQL  dq.*          Pushgateway ─► Prometheus ◄── StatsD exporter
 (history, bad row IDs)                        │  │
        │                                      │  └─► Alertmanager ─► webhook / Slack
        └──────────────► Grafana ◄─────────────┘
```

## What it checks

| Dimension | Check type (YAML) | Great Expectations expectation |
|---|---|---|
| Duplicates | `unique`, `unique_together` | `ExpectColumnValuesToBeUnique`, `ExpectCompoundColumnsToBeUnique` |
| Missing values | `not_null` (+ `mostly`) | `ExpectColumnValuesToNotBeNull` |
| Schema consistency | `schema_check` | `ExpectTableColumnsToMatchSet` (exact) + `ExpectColumnValuesToBeInTypeList` |
| Referential integrity | `referential` | parent-key lookup → `ExpectColumnValuesToBeInSet` on an orphan flag |
| Data freshness | `freshness` (`max_age_hours`) | record-age column → `ExpectColumnMinToBeBetween` |
| Validity | `regex`, `in_set`, `between` | `…MatchRegex`, `…BeInSet`, `…BeBetween` |
| Volume | `row_count` | `ExpectTableRowCountToBeBetween` |

Each check has a **severity**: `critical` fails the Airflow task and blocks `publish_certified_data`;
`warning` is recorded, graphed and alerted on but does not block.
For every failure the platform stores the **primary keys of the offending rows** and a sample of the bad values.

## Quick start

Requires Docker + Docker Compose.

```bash
make up                 # builds the Airflow image and starts ~10 containers (first build takes a few minutes)
make seed-dirty         # load demo data containing every kind of defect
make run                # or wait: the DAG runs every 10 minutes
```

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Grafana (folder *Data Quality*) | http://localhost:3000 | admin / admin |
| Prometheus / Alerts | http://localhost:9090 | |
| Alertmanager | http://localhost:9093 | |

`make logs-alerts` shows alerts as Alertmanager delivers them.

## Demo scenarios

```bash
make seed-clean   # all checks pass; DAG run succeeds; publish_certified_data runs
make seed-dirty   # duplicates, nulls, malformed emails, orphan FKs, negative prices, invalid status...
make seed-stale   # data stopped arriving -> freshness breach (orders=critical, products=warning)
make seed-drift   # upstream added a column and changed order_total to TEXT -> schema failure
```
After seeding, run `make run`. With `dirty`/`stale`/`drift` the DAG run **fails at the quality gate** and
`publish_certified_data` is not executed; Grafana shows the failing checks with sample bad IDs, and
`DQCriticalCheckFailed` / `DQFreshnessBreached` fire within ~1–2 minutes.
You can also trigger the `dq_load_demo_data` DAG from the Airflow UI to switch scenarios.

## Repo layout

```
config/expectations.yaml   the data-quality contract (add tables/checks here, no code needed)
dq_platform/               Python package: suite_builder, runner, store, metrics, cli
dags/dq_pipeline.py        validation DAG (+ dq_load_demo_data.py)
scripts/seed_data.py       demo warehouse + defect injection
monitoring/                Prometheus rules, Alertmanager, StatsD mapping, Grafana dashboard (provisioned)
tests/                     unit tests + optional PostgreSQL integration test
```

## Metrics (Prometheus)

Pushed per table (label `table`) by the validation tasks via the Pushgateway:
`dq_table_pass_rate`, `dq_table_status`, `dq_table_checks_total`, `dq_table_failed_checks`,
`dq_table_critical_failed_checks`, `dq_failed_checks{dimension,severity}`, `dq_check_success{check,…}`,
`dq_check_unexpected_percent`, `dq_freshness_age_hours`, `dq_freshness_threshold_hours`, `dq_table_rows`,
`dq_validation_duration_seconds`, `dq_validation_last_run_timestamp_seconds`; pipeline-level:
`dq_pipeline_pass_rate`, `dq_pipeline_critical_failed_checks`.
Airflow itself reports through StatsD: `airflow_dagrun_duration_seconds`, `airflow_ti_finish_total{state}`,
`airflow_scheduler_heartbeat_total`.

**Alerts** (`monitoring/prometheus/alerts.yml`): critical check failed, pass rate < 90 %, freshness breached,
validation stale (> 1 h), metrics missing, Airflow task failures, scheduler heartbeat lost, scrape target down.
To send to Slack, uncomment the `slack_configs` block in `monitoring/alertmanager/alertmanager.yml`.

## Adding a check

```yaml
# config/expectations.yaml
orders:
  checks:
    - {type: between, column: order_total, min: 0, max: 1000000, severity: critical}
    - {type: referential, column: customer_id, ref_table: customers, ref_column: customer_id}
```
The DAG creates one task per table in the file; new tables appear automatically.

## Run without Docker / tests

```bash
pip install -r requirements-dev.txt
pytest                                   # unit tests (SQLite + in-memory), no services needed
DQ_TEST_URL=postgresql+psycopg2://user:pw@localhost:5432/warehouse pytest   # + PostgreSQL integration
python -m scripts.seed_data --scenario dirty
python -m dq_platform.cli run --table all
```
CLI exit codes: `0` ok · `1` critical data-quality failure · `2` infrastructure error (Airflow retries only this one).

## Design notes

* **Isolated GX environment.** Great Expectations runs from a dedicated venv (`/opt/dq-venv`) inside the Airflow
  image, so its dependency tree never conflicts with Airflow's pinned packages. The DAG calls the CLI as a subprocess.
* **Pushgateway for DQ metrics** because tasks are short-lived; Airflow's own metrics use StatsD → Prometheus.
* **Results in Postgres as well as Prometheus**: Prometheus answers "how is it trending?", Postgres answers
  "which exact rows failed in run X?".
* **Idempotent**: re-running a task (Airflow retry) replaces that table's results for the run.
* **Scaling note**: tables are loaded into pandas, which is ideal up to a few million rows. For larger tables, switch
  `data.py` to a GX SQL datasource (or sample/partition by `updated_at`) — the YAML contract and everything downstream stay the same.
