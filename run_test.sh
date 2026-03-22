#!/bin/bash

cd integration_tests

echo "Starting integration tests"
set -eo pipefail
source test.env
source vars.env

echo "=== Installing dependencies ==="
uv run dbt deps

echo "=== Setting up test source tables ==="
uv run dbt run-operation prep_structure

echo "=== Running dbt (creates tables, streams, and tasks) ==="
uv run dbt run --full-refresh

echo "=== Running structural tests (table/stream/task existence) ==="
uv run dbt test --select tag:structural

echo "=== Inserting new data to test stream processing ==="
uv run dbt run-operation insert_data

echo "=== Waiting for tasks to process stream data (up to 120s) ==="
sleep 120

echo "=== Running data tests (verifying stream data was merged) ==="
uv run dbt test --select tag:data

echo "=== All integration tests passed ==="
