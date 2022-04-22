{{ config(enabled=var('using_logs', True)) }}

with base as (
  select * 
  from {{ var('logs') }}
),

final as (
    select *
    from base
)

select * 
from final