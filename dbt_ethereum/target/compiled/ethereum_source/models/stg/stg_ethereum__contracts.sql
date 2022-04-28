with base as (
  select * 
  from ethereum.contracts
),

final as (
    select *
    from base
)

select * 
from final