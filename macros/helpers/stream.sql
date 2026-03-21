{%- macro stream_source(source_name, table_name) -%}
    {{ dbt_snowflake_streams_tasks.stream('source', source_name, table_name) }}
{%- endmacro -%}

{%- macro stream_ref(model_name) -%}
    {{ dbt_snowflake_streams_tasks.stream('model', none, model_name) }}
{%- endmacro -%}


{%- macro stream(model_type, source_name, model_name) -%}

    {%- set full_refresh = flags.FULL_REFRESH == TRUE -%}

    {# Store source config for the materialization to use #}
    {% do config.set('src_table', {
        'model_type': model_type,
        'source_name': source_name,
        'model_name': model_name
    }) %}

    {%- if model_type == 'model' -%}
        {%- set target_model = ref(model_name) -%}
    {% elif model_type == 'source' %}
        {%- set target_model = source(source_name, model_name) -%}
    {%- else -%}
        {%- do exceptions.raise_compiler_error("Invalid model_type provided to stream macro. Use 'model' or 'source'.") -%}
    {%- endif -%}

    {# During full refresh or initial table creation, reference the source directly #}
    {# During normal runs, the task will use the stream - but for the SQL compilation #}
    {# we still reference the source so dbt can discover the columns via a temp view #}
    {%- do log('Stream macro - target model: ' ~ target_model, info=true ) -%}
    {{- target_model -}}

{%- endmacro -%}
