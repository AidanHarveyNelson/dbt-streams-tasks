select *
from {{ dbt_snowflake_streams_tasks.stream_source('customers', 'testing') }}
