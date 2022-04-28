{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['dt'],
        file_format='parquet',
        pre_hook={
            'sql': 'create or replace function looksrare_looksrareexchange_matchaskwithtakerbid_calldecodeudf as "io.iftech.sparkudf.hive.Looksrare_LooksRareExchange_matchAskWithTakerBid_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.0.jar";'
        }
    )
}}

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        looksrare_looksrareexchange_matchaskwithtakerbid_calldecodeudf(unhex_input, unhex_output, '{"inputs": [{"components": [{"internalType": "bool", "name": "isOrderAsk", "type": "bool"}, {"internalType": "address", "name": "taker", "type": "address"}, {"internalType": "uint256", "name": "price", "type": "uint256"}, {"internalType": "uint256", "name": "tokenId", "type": "uint256"}, {"internalType": "uint256", "name": "minPercentageToAsk", "type": "uint256"}, {"internalType": "bytes", "name": "params", "type": "bytes"}], "internalType": "struct OrderTypes.TakerOrder", "name": "takerBid", "type": "tuple"}, {"components": [{"internalType": "bool", "name": "isOrderAsk", "type": "bool"}, {"internalType": "address", "name": "signer", "type": "address"}, {"internalType": "address", "name": "collection", "type": "address"}, {"internalType": "uint256", "name": "price", "type": "uint256"}, {"internalType": "uint256", "name": "tokenId", "type": "uint256"}, {"internalType": "uint256", "name": "amount", "type": "uint256"}, {"internalType": "address", "name": "strategy", "type": "address"}, {"internalType": "address", "name": "currency", "type": "address"}, {"internalType": "uint256", "name": "nonce", "type": "uint256"}, {"internalType": "uint256", "name": "startTime", "type": "uint256"}, {"internalType": "uint256", "name": "endTime", "type": "uint256"}, {"internalType": "uint256", "name": "minPercentageToAsk", "type": "uint256"}, {"internalType": "bytes", "name": "params", "type": "bytes"}, {"internalType": "uint8", "name": "v", "type": "uint8"}, {"internalType": "bytes32", "name": "r", "type": "bytes32"}, {"internalType": "bytes32", "name": "s", "type": "bytes32"}], "internalType": "struct OrderTypes.MakerOrder", "name": "makerAsk", "type": "tuple"}], "name": "matchAskWithTakerBid", "outputs": [], "stateMutability": "nonpayable", "type": "function"}', 'matchAskWithTakerBid') as data
    from {{ ref('stg_ethereum__traces') }}
    where to_address = lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a")
    and address_hash = abs(hash(lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a"))) % 10
    and selector = "0x30783338"
    and selector_hash = abs(hash("0x30783338")) % 10

    {% if is_incremental() %}
      and dt = '{{ var("dt") }}'
    {% endif %}
),

final as (
    select
        call_success,
        call_block_number,
        call_block_time,
        call_trace_address,
        call_tx_hash,
        contract_address,
        dt,
        data.input.*,
        data.output.*
    from base
)

select /* REPARTITION(dt) */ *
from final
