with opensea_trades as (
  select *
  from {{ ref('opensea_trades') }}
),

cryptopunks_trades as (
  select *
  from {{ ref('cryptopunks_trades') }}
),

final as (
  select
    'OpenSea' as platform,
    nft_token_id,
    exchange_contract_address,
    nft_contract_address,
    erc_standard,
    number_of_items,
    aggregator,
    trade_type,
    buyer,
    seller,
    nft_project_name,
    currency_amount,
    currency_symbol,
    currency_contract,
    usd_amount,
    eth_amount,
    original_currency_amount,
    original_currency_contract,
    block_time,
    block_number,
    tx_hash,
    tx_from,
    tx_to
  from opensea_trades

  union all

  select
    platform,
    nft_token_id,
    exchange_contract_address,
    nft_contract_address,
    erc_standard,
    number_of_items,
    aggregator,
    trade_type,
    buyer,
    seller,
    nft_project_name,
    currency_amount,
    currency_symbol,
    currency_contract,
    usd_amount,
    eth_amount,
    original_currency_amount,
    original_currency_contract,
    block_time,
    block_number,
    tx_hash,
    tx_from,
    tx_to
  from cryptopunks_trades
)

select /*+ REPARTITION(50) */ *
from final
