{% macro insert_data() %}

    {% set tables = [
        [target.database, target.schema, 'testing_source']|join('.'),
        [target.database, target.schema, 'testing_ref']|join('.')
    ] %}

    {% for table in tables %}
        {% set insert_data_sql %}
            INSERT INTO {{ table }} VALUES (2, 'new stream data')
        {% endset %}

        {% do log('Inserting data into ' ~ table, info = true) %}
        {% do run_query(insert_data_sql) %}
    {% endfor %}

{% endmacro %}
