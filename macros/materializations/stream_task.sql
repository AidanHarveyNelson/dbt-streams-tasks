{#
    This materialization creates a Snowflake stream + task pipeline in dbt.
    
    The user writes SQL representing the desired output schema (SELECTing from a stream_ref/stream_source).
    This materialization:
      1. Creates a temp view to discover the output columns
      2. Creates the target table (if new or full_refresh) using that schema
      3. Creates a Snowflake stream on the source table/view
      4. Creates a Snowflake task with a MERGE statement to load stream data into the target table
      5. Resumes the task so it runs automatically

    Configuration references:
      https://docs.snowflake.com/en/sql-reference/sql/create-stream
      https://docs.snowflake.com/en/sql-reference/sql/create-task
#}

{%- materialization stream_task, adapter = "snowflake" -%}

{%- set target_relation = this -%}
{%- set unique_key = config.get('unique_key') -%}
{%- set existing_relation = load_relation(target_relation) -%}
{%- set is_new = existing_relation is none or flags.FULL_REFRESH == true -%}

{%- do log('stream_task: target=' ~ target_relation ~ ' existing=' ~ existing_relation ~ ' is_new=' ~ is_new, info=true) -%}

{# Resolve the stream source relation #}
{%- set stream_target_config = config.get('src_table') -%}
{%- if stream_target_config is none -%}
    {%- do exceptions.raise_compiler_error("stream_task materialization requires using stream_ref() or stream_source() in your SQL to specify the stream source.") -%}
{%- endif -%}

{%- if stream_target_config.model_type == 'source' -%}
    {%- set stream_source_relation = load_relation(source(stream_target_config.source_name, stream_target_config.model_name)) -%}
{%- else -%}
    {%- set stream_source_relation = load_relation(ref(stream_target_config.model_name)) -%}
{%- endif -%}

{%- if stream_source_relation is none -%}
    {%- do exceptions.raise_compiler_error("stream_task: Could not find the source relation for the stream. Make sure the source/ref model exists.") -%}
{%- endif -%}

{%- set stream_config = config.require('stream') -%}
{%- set task_config = config.require('task') -%}
{%- set stream_name = stream_config.name -%}
{%- set task_name = task_config.name -%}

{%- do log('stream_task: stream_source=' ~ stream_source_relation ~ ' stream_name=' ~ stream_name ~ ' task_name=' ~ task_name, info=true) -%}

-- Run Pre-Hooks
{{- run_hooks(pre_hooks) -}}

{# 
    Step 1: Create a temporary view from the user's SQL to discover the output schema.
    The user's SQL references the source table directly (stream_ref/stream_source returns the source during compilation).
#}
{%- set tmp_relation = make_temp_relation(target_relation).incorporate(type='view') -%}

{%- call statement('create_tmp_relation') -%}
    CREATE OR REPLACE TEMPORARY VIEW {{ tmp_relation }} AS {{ sql }}
{%- endcall -%}

{#
    Step 2: Get columns from the temp view (excluding stream metadata columns)
#}
{%- set columns = adapter.get_columns_in_relation(tmp_relation)
    | rejectattr('name', 'equalto', 'METADATA$ACTION')
    | rejectattr('name', 'equalto', 'METADATA$ISUPDATE')
    | rejectattr('name', 'equalto', 'METADATA$ROW_ID')
    | list -%}

{%- do log('stream_task: discovered ' ~ columns|length ~ ' columns', info=true) -%}

{#
    Step 3: Create or alter the target table
#}
{%- call statement('create_table') -%}
    {{ dbt_snowflake_streams_tasks.create_table(target_relation, sql, tmp_relation, is_new) }}
{%- endcall -%}

{#
    Step 4: Create the stream on the source table/view
#}
{%- call statement('create_stream') -%}
    {{ dbt_snowflake_streams_tasks.create_stream(stream_source_relation) }}
{%- endcall -%}

{#
    Step 5: Create the task with a MERGE statement
#}
{%- call statement('create_task') -%}
    {{ dbt_snowflake_streams_tasks.create_task(stream_name, target_relation, sql, tmp_relation, unique_key) }}
{%- endcall -%}

{#
    Step 6: Resume the task so it runs automatically
#}
{%- call statement('resume_task') -%}
    ALTER TASK IF EXISTS {{ task_name }} RESUME
{%- endcall -%}

{{- adapter.commit() -}}

-- Run Post-Hooks
{{- run_hooks(post_hooks) -}}

{%- do drop_relation_if_exists(tmp_relation) -%}

{# Use persist_docs to handle any documentation #}

{%- call statement('main') -%}
    SELECT 1
{%- endcall -%}

-- Return the relations created in this materialization
{{- return({'relations': [target_relation]}) -}}

{%- endmaterialization -%}
