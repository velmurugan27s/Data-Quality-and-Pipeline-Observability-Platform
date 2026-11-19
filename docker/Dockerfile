FROM apache/airflow:2.10.5-python3.11

USER root
# Isolated environment for Great Expectations (avoids dependency conflicts with Airflow's pinned stack)
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m venv /opt/dq-venv \
 && /opt/dq-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt \
 && chown -R airflow /opt/dq-venv

USER airflow
# StatsD client so Airflow can emit scheduler/DAG/task metrics (-> statsd-exporter -> Prometheus)
RUN pip install --no-cache-dir statsd
ENV DQ_HOME=/opt/dq DQ_PYTHON=/opt/dq-venv/bin/python GX_ANALYTICS_ENABLED=false
