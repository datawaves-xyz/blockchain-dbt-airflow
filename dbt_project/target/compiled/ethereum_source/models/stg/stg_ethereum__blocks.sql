with base as (
  select * 
  from ethereum.blocks
),

final as (
    select *
    from base
)

select * 
from final