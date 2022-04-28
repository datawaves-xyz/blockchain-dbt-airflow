

with base as (
  select * 
  from ethereum.traces
),

final as (
    select *
    from base
)

select * 
from final