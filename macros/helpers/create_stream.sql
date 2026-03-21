{%- macro create_stream(source_table) -%}
    {%- do log('create_stream: source_table=' ~ source_table, info=true) -%}
    {%- set stream_config = config.require('stream') -%}
    {%- set name = stream_config.name -%}
    {%- set append_only = stream_config.get('append_only', true) -%}
    {%- set show_initial_rows = stream_config.get('show_initial_rows', true) -%}
    {%- set copy_grants = stream_config.get('copy_grants', false) -%}
    {%- set description = stream_config.get('description', '') -%}

    {%- if source_table.is_view -%}
        {%- set on_type = 'VIEW' -%}
    {%- elif source_table.is_table -%}
        {%- set on_type = 'TABLE' -%}
    {%- else -%}
        {%- do exceptions.raise_compiler_error("create_stream: source must be a table or view. Got: " ~ source_table ~ " type=" ~ source_table.type) -%}
    {%- endif -%}

    CREATE OR REPLACE STREAM {{ name }}
    {{ "COPY GRANTS" if copy_grants else '' }}
    ON {{ on_type }} {{ source_table }}
    COMMENT = '{{ description }}'
    APPEND_ONLY = {{ append_only | upper }}
    SHOW_INITIAL_ROWS = {{ show_initial_rows | upper }}
{%- endmacro -%}
