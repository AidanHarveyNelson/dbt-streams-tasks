{%- macro create_table(target_table, tmp_relation, is_new) -%}
    {%- do log('create_table: target=' ~ target_table ~ ' is_new=' ~ is_new, info=true) -%}

    {%- if is_new -%}
        {# Create the target table using the schema from the temp view, populated with initial data #}
        CREATE OR REPLACE TABLE {{ target_table }} LIKE {{ tmp_relation }}
    {%- else -%}
        {# Table exists - check for new columns and add them #}
        {%- set missing_columns = adapter.get_missing_columns(tmp_relation, target_table) -%}
        {%- do log('create_table: missing_columns=' ~ missing_columns, info=true) -%}
        {%- if missing_columns | length > 0 -%}
            ALTER TABLE {{ target_table }}
            {%- for col in missing_columns %}
                ADD COLUMN "{{ col.name }}" {{ col.data_type }}{{ ";" if loop.last else "" }}
            {%- endfor %}
        {%- else -%}
            SELECT 1 {# No schema changes needed #}
        {%- endif -%}
    {%- endif -%}
{%- endmacro -%}
