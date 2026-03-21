{% macro prep_structure() %}

    {% set create_schema_sql = "CREATE SCHEMA IF NOT EXISTS " ~ target.database ~ "." ~ target.schema %}

    {% do log('Creating schema ' ~ target.schema, info = true) %}
    {% do run_query(create_schema_sql) %}

    {% set test_tables  = [
        [target.database, target.schema, 'testing_source']|join('.'),
        [target.database, target.schema, 'testing_ref']|join('.'),
    ] %}

    {% for table in test_tables %}
        {% set create_testing_table_sql %}
            CREATE OR REPLACE TABLE {{ table }} (
                id INTEGER,
                data STRING
            )
            CHANGE_TRACKING = TRUE
        {% endset %}

        {% do log('Creating testing table ' ~ table, info = true) %}
        {% do run_query(create_testing_table_sql) %}
    {% endfor %}

    {# Insert initial data into both source tables #}
    {% for table in test_tables %}
        {% set insert_data_sql %}
            INSERT INTO {{ table }} VALUES (1, 'initial data')
        {% endset %}

        {% do log('Inserting initial data into ' ~ table, info = true) %}
        {% do run_query(insert_data_sql) %}
    {% endfor %}

{% endmacro %}
