with base as (
  select * 
  from ethereum.transactions
),

final as (
    select *
    from base
)

select * 
from final