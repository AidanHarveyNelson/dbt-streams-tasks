{%- macro create_table(target_table, sql, tmp_relation, is_new) -%}
    {%- do log('Target Table is: ' ~ target_table, info = true) -%}
    {%- set run_sql -%}
        {%- if not is_new -%}
            {% set missing_columns = adapter.get_missing_columns(target_table, tmp_relation) %}
            {% do log('Missing columns are ' ~ missing_columns, info=true)%}
            {% if missing_columns %}
                alter table {{ target_table }}
                {% for col in missing_columns %}
                    add column "{{- col.name -}}" {{- col.data_type -}}{{- ";" if loop.last else "\n" -}}
                {% endfor %}
            {% endif%}
        {%- else -%}
            create table {{ target_table }} as {{ sql }};
        {%- endif -%}
    {%- endset -%}
    {{ run_sql }}
{%- endmacro -%}
