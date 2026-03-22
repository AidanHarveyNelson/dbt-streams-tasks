-- Test that the task transform_from_ref_test was created

{% set query %}
    SHOW TASKS LIKE 'transform_from_ref_test' IN SCHEMA {{ target.database }}.{{ target.schema }}
{% endset %}

{% set results = run_query(query) %}

{% if execute %}
    {% if results | length == 0 %}
        {{ exceptions.raise_compiler_error("Task 'transform_from_ref_test' was not created in schema " ~ target.schema) }}
    {% endif %}
{% endif %}

SELECT 1 WHERE FALSE
