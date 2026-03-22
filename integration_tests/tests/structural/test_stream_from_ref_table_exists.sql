-- Test that the stream_from_ref target table was created and has the expected columns

{% set expected_columns = ['ID', 'DATA'] %}

{% set query %}
    SELECT column_name
    FROM {{ target.database }}.information_schema.columns
    WHERE table_schema = upper('{{ target.schema }}')
      AND table_name = 'TABLE_FROM_REF'
    ORDER BY ordinal_position
{% endset %}

{% set results = run_query(query) %}

{% if execute %}
    {% set actual_columns = results.columns[0].values() %}
    {% for col in expected_columns %}
        {% if col not in actual_columns %}
            {{ exceptions.raise_compiler_error("Column " ~ col ~ " not found in STREAM_FROM_REF table. Actual columns: " ~ actual_columns) }}
        {% endif %}
    {% endfor %}
{% endif %}

-- Return 0 rows to pass
SELECT 1 WHERE FALSE
