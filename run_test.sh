#!/bin/bash

cd integration_tests

echo "Starting integration tests"
set -eo pipefail
source test.env
source vars.env
echo "Building dbt project"
uv run dbt deps
echo "Running dbt run-operation prep_structure"
uv run dbt run-operation prep_structure
uv run dbt run
uv run dbt run-operation insert_data
sleep 120
uv run dbt test
