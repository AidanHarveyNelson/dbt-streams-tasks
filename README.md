# dbt-snowflake-streams-tasks

A dbt package that enables creating Snowflake [streams](https://docs.snowflake.com/en/sql-reference/sql/create-stream) and [tasks](https://docs.snowflake.com/en/sql-reference/sql/create-task) through dbt, providing an alternative to incremental models that leverages Snowflake's native change data capture.

## How It Works

Instead of scheduling dbt runs to process incremental data, this package creates:

1. **A target table** — with the schema defined by your SQL
2. **A Snowflake stream** — to capture changes on your source table/view
3. **A Snowflake task** — with a MERGE statement that automatically processes stream data into your target table

Once deployed, the stream and task run autonomously in Snowflake — no dbt scheduling required.

## Installation

Add to your `packages.yml`:

```yaml
packages:
  - git: "https://github.com/<your-org>/dbt-snowflake-streams-tasks.git"
    revision: v0.0.1
```

## Usage

### 1. Write Your Model SQL

Use `stream_ref()` or `stream_source()` to specify the stream source:

```sql
-- models/my_model.sql
select
    id,
    name,
    email,
    created_at
from {{ dbt_streams_tasks.stream_ref('upstream_model') }}
```

Or from a source:

```sql
-- models/my_model.sql
select *
from {{ dbt_streams_tasks.stream_source('my_source', 'my_table') }}
```

### 2. Configure the Model

In your YAML file:

```yaml
# models/_models.yml
version: 2

models:
  - name: my_model
    config:
      materialized: stream_task
      unique_key:
        - id
      stream:
        name: "my_stream"
        description: "Captures changes from the source"
        append_only: true
        show_initial_rows: true
        copy_grants: false
      task:
        name: "my_task"
        description: "Merges stream data into target"
        warehouse: "MY_WAREHOUSE"
        schedule: "1 MINUTE"
        when: "SYSTEM$STREAM_HAS_DATA('my_stream')"
```

### 3. Run dbt

```bash
dbt run            # Creates table, stream, and task
dbt run --full-refresh  # Recreates everything from scratch
```

## Configuration Reference

### Stream Config

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `name` | Yes | - | Name of the Snowflake stream |
| `description` | No | `''` | Comment on the stream |
| `append_only` | No | `true` | Only track inserts (not updates/deletes) |
| `show_initial_rows` | No | `true` | Include existing rows on stream creation |
| `copy_grants` | No | `false` | Copy grants when replacing the stream |

### Task Config

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `name` | Yes | - | Name of the Snowflake task |
| `description` | No | `''` | Comment on the task |
| `warehouse` | No* | - | Warehouse to execute the task (*required if not serverless) |
| `schedule` | No | - | How often to run (e.g., `'1 MINUTE'`) |
| `when` | No | - | Condition to check before running |
| `allow_overlapping_execution` | No | - | Allow concurrent executions |
| `user_task_timeout_ms` | No | - | Task timeout in milliseconds |
| `suspend_task_after_num_failures` | No | - | Auto-suspend after N failures |
| `task_auto_retry_attempts` | No | - | Number of auto-retry attempts |

## Running Integration Tests

```bash
# Set up environment variables
cp integration_tests/test.env.sample integration_tests/test.env
cp integration_tests/vars.env.sample integration_tests/vars.env
# Edit both files with your Snowflake credentials

# Run tests
./run_test.sh
```

## Requirements

- dbt-core >= 1.9.1
- dbt-snowflake >= 1.9.0
- Snowflake account with CHANGE_TRACKING enabled on source tables
