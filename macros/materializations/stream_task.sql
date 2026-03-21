{#
    This materialization is used for creating a stream task in dbt.
    Configuration for the streams and tasks are taken from the Snowflake parameters
    https://docs.snowflake.com/en/sql-reference/sql/create-stream
    https://docs.snowflake.com/en/sql-reference/sql/create-task

    Current logic is to always replace the stream and task regardless of if it exists or not.

    To-Do change this logic to alter if already exists and create a way to alter the table if column definition has changed.
    A good option would be to create a dummy table with the new SQL and compare the columns between the old table and new and then use that comparison
    to construct the alter logic
#}

{%- materialization stream_task, adapter = "snowflake" -%}

{%- do log('Entering Materialization', info = true) -%}

{%- set target_relation = this -%}
{%- set unique_key = config.get('unique_key') -%}
{%- set incremental_strategy = config.get('incremental_strategy') or 'default' -%}
{%- set tmp_relation_type = dbt_snowflake_get_tmp_relation_type(incremental_strategy, unique_key, 'sql') -%}
{%- set tmp_relation = make_temp_relation(target_relation).incorporate(type=tmp_relation_type) -%}
{%- set existing_relation = load_relation(target_relation) -%}

{%- do log('Target Relation: ' ~ target_relation, info = true) -%}
{%- do log('Existing Relation: ' ~ existing_relation, info = true) -%}

{%- set is_new = existing_relation is none or flags.FULL_REFRESH == true -%}

{%- do log('The target object is new? ' ~ is_new, info = true) -%}

-- Run Pre-Hooks
{{- run_hooks(pre_hooks) -}}

{%- call statement('main') -%}
    {%- do log('Creating Objects for Relation: ' ~ target_relation ~ '\n' ~ sql, info = true) -%}
    {%- set stream_target_config = config.get('src_table') -%}
    {%- do log('Logging Stream Target Config: ' ~ stream_target_config, info = true) -%}

    {%- if stream_target_config.model_type == 'source' -%}
        {%- set stream_source = load_relation(source(stream_target_config.source_name, stream_target_config.model_name)) -%}
    {%- else -%}
        {%- set stream_source = load_relation(ref(stream_target_config.model_name)) -%}
    {%- endif -%}

    {%- set stream_name = config.require('stream').name -%}

    {%- do log('Logging Stream Source: ' ~ stream_source, info = true) -%}
    {%- call statement('create_tmp_relation') -%}
        {{- snowflake__create_view_as_with_temp_flag(tmp_relation, sql, True) -}}
    {%- endcall -%}
    {%- set stream_query = dbt_snowflake_streams_tasks.create_stream(stream_source) -%}
    {%- do log('Logging Stream Query: ' ~ stream_query, info = true) -%}
    {%- set task_query = dbt_snowflake_streams_tasks.create_task(stream_name, target_relation, sql, tmp_relation, unique_key) -%}
    {%- do log('Logging Task Query: ' ~ task_query, info = true) -%}
    {%- set table_query = dbt_snowflake_streams_tasks.create_table(target_relation, sql, tmp_relation, is_new) -%}
    {%- do log('Logging Table Query: ' ~ table_query, info = true) -%}
    {{- stream_query ~ ";\n\n" ~ task_query ~ ";\n\n" ~ table_query -}}
{%- endcall -%}

{{- adapter.commit() -}}

{{- run_hooks(post_hooks) -}}

{%- do drop_relation_if_exists(tmp_relation) -%}

-- Return the relations created in this materialization
{{- return({'relations': [target_relation]}) -}}

{%- endmaterialization -%}
