{%- macro create_task(task_name, stream, target_table, sql, tmp_relation, unique_key, stream_source_relation) -%}
    {%- set task_config = config.require('task') -%}
    {%- set stream_config = config.require('stream') -%}

    {%- do log('create_task: name=' ~ task_name ~ ' stream=' ~ stream ~ ' target=' ~ target_table, info=true) -%}

    {# Get columns from tmp relation, excluding stream metadata columns #}
    {%- set columns = adapter.get_columns_in_relation(tmp_relation)
        | rejectattr('name', 'equalto', 'METADATA$ACTION')
        | rejectattr('name', 'equalto', 'METADATA$ISUPDATE')
        | rejectattr('name', 'equalto', 'METADATA$ROW_ID')
        | list -%}

    {# Build the stream SQL by replacing the rendered source relation with the stream name #}
    {%- set rendered_source = stream_source_relation | string | replace('"', '') | lower -%}
    {%- set stream_sql = sql | lower | replace(rendered_source, stream) -%}

    {# Build the MERGE statement that the task will execute #}
    {%- set merge_sql -%}
        MERGE INTO {{ target_table }} AS TARGET
        USING ({{ stream_sql }}) AS SOURCE
        ON (
            {%- for key in unique_key %}
            TARGET.{{ key }} = SOURCE.{{ key }}{{ " AND" if not loop.last else "" }}
            {%- endfor %}
        )
        WHEN MATCHED AND SOURCE.METADATA$ACTION = 'DELETE' AND SOURCE.METADATA$ISUPDATE = 'FALSE' THEN
            DELETE
        WHEN MATCHED AND SOURCE.METADATA$ACTION = 'INSERT' THEN
            UPDATE SET
            {%- for col in columns %}
                TARGET.{{ col.name }} = SOURCE.{{ col.name }}{{ "," if not loop.last else "" }}
            {%- endfor %}
        WHEN NOT MATCHED AND SOURCE.METADATA$ACTION = 'INSERT' THEN
            INSERT (
                {%- for col in columns %}
                {{ col.name }}{{ "," if not loop.last else "" }}
                {%- endfor %}
            )
            VALUES (
                {%- for col in columns %}
                SOURCE.{{ col.name }}{{ "," if not loop.last else "" }}
                {%- endfor %}
            )
    {%- endset -%}

    {# Build task with optional parameters #}
    CREATE OR REPLACE TASK {{ task_name }}
    {%- if task_config.get('warehouse') %}
        WAREHOUSE = {{ task_config.warehouse }}
    {%- endif %}
    {%- if task_config.get('schedule') %}
        SCHEDULE = '{{ task_config.schedule }}'
    {%- endif %}
    {%- if task_config.get('description') %}
        COMMENT = '{{ task_config.description }}'
    {%- endif %}
    {%- if task_config.get('allow_overlapping_execution') is not none %}
        ALLOW_OVERLAPPING_EXECUTION = {{ task_config.allow_overlapping_execution | upper }}
    {%- endif %}
    {%- if task_config.get('user_task_timeout_ms') %}
        USER_TASK_TIMEOUT_MS = {{ task_config.user_task_timeout_ms }}
    {%- endif %}
    {%- if task_config.get('suspend_task_after_num_failures') %}
        SUSPEND_TASK_AFTER_NUM_FAILURES = {{ task_config.suspend_task_after_num_failures }}
    {%- endif %}
    {%- if task_config.get('error_integration') %}
        ERROR_INTEGRATION = '{{ task_config.error_integration }}'
    {%- endif %}
    {%- if task_config.get('success_integration') %}
        SUCCESS_INTEGRATION = '{{ task_config.success_integration }}'
    {%- endif %}
    {%- if task_config.get('log_level') %}
        LOG_LEVEL = '{{ task_config.log_level }}'
    {%- endif %}
    {%- if task_config.get('task_auto_retry_attempts') %}
        TASK_AUTO_RETRY_ATTEMPTS = {{ task_config.task_auto_retry_attempts }}
    {%- endif %}
    {%- if task_config.get('user_task_minimum_trigger_interval_in_seconds') %}
        USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = {{ task_config.user_task_minimum_trigger_interval_in_seconds }}
    {%- endif %}
    {%- if task_config.get('when') %}
    WHEN
        {{ task_config.when }}
    {%- endif %}
    AS
        {{ merge_sql }}
{%- endmacro -%}
