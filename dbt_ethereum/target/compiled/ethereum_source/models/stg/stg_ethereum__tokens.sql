with base as (
  select * 
  from ethereum.tokens
),

final as (
  select
    base.address as contract_address,
    base.symbol,
    base.name,
    cast(base.decimals as 
    int
) as decimals,
    cast(base.total_supply as 
    int
) as total_supply
  from base
)

select * 
from final