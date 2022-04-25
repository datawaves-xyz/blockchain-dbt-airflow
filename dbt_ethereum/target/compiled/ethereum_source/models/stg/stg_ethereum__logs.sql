

with base as (
  select * 
  from ethereum.logs
),

final as (
    select *
    from base
)

select * 
from final