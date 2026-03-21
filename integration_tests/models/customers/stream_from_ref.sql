select *
from {{ dbt_snowflake_streams_tasks.stream_ref('view_on_source') }}
