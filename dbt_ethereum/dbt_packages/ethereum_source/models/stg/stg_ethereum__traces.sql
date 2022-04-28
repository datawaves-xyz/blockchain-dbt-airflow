{{ config(enabled=var('using_traces', True)) }}

with base as (
  select * 
  from {{ var('traces') }}
),

final as (
    select *
    from base
)

select * 
from final