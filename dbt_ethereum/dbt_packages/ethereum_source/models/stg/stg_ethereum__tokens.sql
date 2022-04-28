with base as (
  select * 
  from {{ var('tokens') }}
),

final as (
  select
    base.address as contract_address,
    base.symbol,
    base.name,
    cast(base.decimals as {{ dbt_utils.type_int() }}) as decimals,
    cast(base.total_supply as {{ dbt_utils.type_int() }}) as total_supply
  from base
)

select * 
from final