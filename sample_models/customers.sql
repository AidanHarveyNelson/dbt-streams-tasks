select *
from {{ dbt_streams_tasks.stream_source('customers', 'testing') }}
