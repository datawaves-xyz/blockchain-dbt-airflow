

with  __dbt__cte__opensea_trades as (
with agg as (
  select * 
  from ethereum_nft.aggregators
), tokens as (
  select * 
  from ethereum_stg_ethereum.stg_ethereum__tokens
),

erc721_token_transfers as (
  select *
  from ethereum_stg_ethereum.stg_ethereum__token_transfers
  where dt >= '1970-01-01'
    and dt < '9999-01-01'
),

wyvern_data as (
  select *
  from ethereum_nft.wyvern_data
  where dt >= '1970-01-01'
    and dt < '9999-01-01'
),

prices_usd as (
  select *
  from prices.usd
  where dt >= '1970-01-01'
    and dt < '9999-01-01'
),

-- Count number of token IDs in each transaction
erc721_tokens_in_tx as (
  select
    transaction_hash as tx_hash,
    cast(round(value, 0) as string) as token_id,
    count(1) as num
  from erc721_token_transfers
  where from_address != '0x0000000000000000000000000000000000000000'
  group by transaction_hash, cast(round(value, 0) as string)
),

-- Count number of token transfers in each transaction;
-- We use this to count number of erc721 and erc1155 items when there's no token_id associated
transfers_in_tx as (
  select
    transaction_hash as tx_hash,
    count(1) as num
  from erc721_token_transfers
  where from_address != '0x0000000000000000000000000000000000000000'
  group by transaction_hash
)

select
  w.token_id as nft_token_id,
  w.exchange_contract_address,
  w.nft_contract_address,
  case
    when erc721_tokens_in_tx.num >= 1 then 'erc721'
    else w.erc_standard
  end as erc_standard,
  -- Count the number of items for different trade types
  case
    when agg.name is null and erc721_tokens_in_tx.num > 1 then erc721_tokens_in_tx.num
    when w.trade_type = 'Single Item Trade' then 1
    else transfers_in_tx.num
  end as number_of_items,
  agg.name as aggregator,
  case
    when agg.name is not null then 'Aggregator Trade'
    when agg.name is null and erc721_tokens_in_tx.num = 1 then 'Single Item Trade'
    when agg.name is null and erc721_tokens_in_tx.num > 1 then 'Bundle Trade'
    else w.trade_type
  end as trade_type,
  -- Replace the buyer when using aggregator to trade
  case when agg.name is not null then w.buyer_when_aggr
    else w.buyer
  end as buyer,
  w.seller,
  -- Get the token of aggregator when using aggregator to trade
  case
    when agg.name is not null then agg_tokens.name
    else tokens.name
  end as nft_project_name,
  -- Adjust the currency amount/symbol with erc20 tokens
  w.currency_amount / power(10, erc20.decimals) as currency_amount,
  w.currency_amount / power(10, erc20.decimals) * p.price as usd_amount,
  w.currency_amount as original_currency_amount,
  case
    when w.original_currency_contract = '0x0000000000000000000000000000000000000000'
      then 'ETH'
    else erc20.symbol
  end as currency_symbol,
  w.currency_contract,
  w.original_currency_contract,
  -- blocks & tx
  w.block_time,
  w.block_number,
  w.tx_hash,
  w.tx_from,
  w.tx_to,
  -- date partition column
  w.dt
from wyvern_data w

left join erc721_tokens_in_tx
  on erc721_tokens_in_tx.tx_hash = w.tx_hash
    and erc721_tokens_in_tx.token_id = w.token_id

left join transfers_in_tx on transfers_in_tx.tx_hash = w.tx_hash

left join prices_usd p
  on p.minute = date_trunc('minute', w.block_time)
    and p.contract_address = w.currency_contract

left join tokens erc20 on erc20.contract_address = w.currency_contract
left join tokens on tokens.contract_address = w.nft_contract_address
left join tokens agg_tokens on agg_tokens.contract_address = w.nft_contract_address
left join agg on agg.contract_address = w.tx_to
),opensea_trades as (
  select *
  from __dbt__cte__opensea_trades
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
    original_currency_amount,
    original_currency_contract,
    block_time,
    block_number,
    tx_hash,
    tx_from,
    tx_to

  from opensea_trades
)

select *
from final