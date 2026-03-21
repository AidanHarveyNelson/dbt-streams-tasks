{% macro cleanup() %}
    {# Clean up all test objects created during integration tests #}

    {% set objects_to_drop = [
        "TASK transform_from_stream_test",
        "TASK transform_from_ref_test",
        "STREAM stream_from_source_test",
        "STREAM stream_from_ref_test",
        "TABLE " ~ [target.database, target.schema, 'STREAM_FROM_SOURCE']|join('.'),
        "TABLE " ~ [target.database, target.schema, 'STREAM_FROM_REF']|join('.'),
        "VIEW " ~ [target.database, target.schema, 'VIEW_ON_SOURCE']|join('.'),
        "TABLE " ~ [target.database, target.schema, 'testing_source']|join('.'),
        "TABLE " ~ [target.database, target.schema, 'testing_ref']|join('.')
    ] %}

    {% for obj in objects_to_drop %}
        {% set drop_sql = "DROP " ~ obj ~ " IF EXISTS" %}
        {% do log('Dropping: ' ~ obj, info=true) %}
        {% do run_query(drop_sql) %}
    {% endfor %}

{% endmacro %}
