#!/bin/bash
# Runs once on first Postgres start: one server, two databases.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-SQL
    CREATE DATABASE airflow;
    CREATE DATABASE warehouse;
SQL
