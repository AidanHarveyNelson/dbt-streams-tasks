select
    *,
    1 as new_column
from {{ dbt_streams_tasks.stream_source('customers', 'testing_source') }}
