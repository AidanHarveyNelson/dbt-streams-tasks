{%- macro create_stream(source_table) -%}
    {%- do log('Source Table is: ' ~ source_table, info = true) -%}
    {%- set name = config.require('stream').name -%}

    {%- set run_sql -%}
        CREATE OR REPLACE STREAM {{ name }}
        {{ "COPY GRANTS" if config.get('stream').copy_grants else '' }}
        {# To-Do Add Tag Config#}
    {%- endset -%}

    {% if source_table.is_view %}
        {%- set run_sql -%}
            {{ run_sql ~ " ON VIEW " ~ source_table }}
            COMMENT = '{{ config.get('stream').description or '' }}'
            APPEND_ONLY = {{ config.get('stream').append_only|upper or 'TRUE' }}
            SHOW_INITIAL_ROWS = {{ config.get('stream').show_initial_rows|upper or 'TRUE' }}
        {%- endset -%}
    {% elif source_table.is_table %}
        {%- set run_sql -%}
            {{ run_sql ~ " ON TABLE " ~ source_table }}
            COMMENT = '{{ config.get('stream').description or '' }}'
            APPEND_ONLY = {{ config.get('stream').append_only|upper or 'TRUE' }}
            SHOW_INITIAL_ROWS = {{ config.get('stream').show_initial_rows|upper or 'TRUE' }}
        {%- endset -%}
    {%- else -%}
        {%- do exceptions.raise_compiler_error("Invalid materialization type provided to stream macro. Use 'view' or 'table'. Object passed in is " ~ source_table ~ " with type: " ~ source_table.type) -%}
    {%- endif -%}
    {%- do log('Creating Stream: ' ~ name, info = true) -%}
    {{ run_sql }}
{%- endmacro -%}
