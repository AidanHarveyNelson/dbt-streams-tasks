-- Test that the stream stream_from_source_test was created
-- Returns 0 rows if the stream exists (pass), or fails with an error if not

{% set query %}
    SHOW STREAMS LIKE 'stream_from_source_test' IN SCHEMA {{ target.database }}.{{ target.schema }}
{% endset %}

{% set results = run_query(query) %}

{% if execute %}
    {% if results | length == 0 %}
        {{ exceptions.raise_compiler_error("Stream 'stream_from_source_test' was not created in schema " ~ target.schema) }}
    {% endif %}
{% endif %}

SELECT 1 WHERE FALSE
