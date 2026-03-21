{{
  config(
    materialized = 'view',
    )
}}

select *
from {{ source('customers', 'testing_ref') }}
