select *
from {{ dbt_streams_tasks.stream_ref('view_on_source') }}
