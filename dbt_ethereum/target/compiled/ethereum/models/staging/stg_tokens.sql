with tokens as (
  select *
  from ethereum_stg_ethereum.stg_ethereum__tokens
)

select
  tokens.address as contract_address,
  tokens.symbol,
  tokens.name,
  cast(tokens.decimals as 
    int
) as decimals,
  cast(tokens.total_supply as 
    int
) as total_supply
from tokens