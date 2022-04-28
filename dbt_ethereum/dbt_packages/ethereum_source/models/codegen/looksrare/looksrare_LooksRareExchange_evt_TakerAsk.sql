{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['dt'],
        file_format='parquet',
        pre_hook={
            'sql': 'create or replace function looksrare_looksrareexchange_takerask_eventdecodeudf as "io.iftech.sparkudf.hive.Looksrare_LooksRareExchange_TakerAsk_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.0.jar";'
        }
    )
}}

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        looksrare_looksrareexchange_takerask_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": false, "internalType": "bytes32", "name": "orderHash", "type": "bytes32"}, {"indexed": false, "internalType": "uint256", "name": "orderNonce", "type": "uint256"}, {"indexed": true, "internalType": "address", "name": "taker", "type": "address"}, {"indexed": true, "internalType": "address", "name": "maker", "type": "address"}, {"indexed": true, "internalType": "address", "name": "strategy", "type": "address"}, {"indexed": false, "internalType": "address", "name": "currency", "type": "address"}, {"indexed": false, "internalType": "address", "name": "collection", "type": "address"}, {"indexed": false, "internalType": "uint256", "name": "tokenId", "type": "uint256"}, {"indexed": false, "internalType": "uint256", "name": "amount", "type": "uint256"}, {"indexed": false, "internalType": "uint256", "name": "price", "type": "uint256"}], "name": "TakerAsk", "type": "event"}', 'TakerAsk') as data
    from {{ ref('stg_ethereum__logs') }}
    where address = lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a")
    and address_hash = abs(hash(lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a"))) % 10
    and selector = "0x68cd251d4d267c6e2034ff0088b990352b97b2002c0476587d0c4da889c11330"
    and selector_hash = abs(hash("0x68cd251d4d267c6e2034ff0088b990352b97b2002c0476587d0c4da889c11330")) % 10

    {% if is_incremental() %}
      and dt = '{{ var("dt") }}'
    {% endif %}
),

final as (
    select
        evt_block_number,
        evt_block_time,
        evt_index,
        evt_tx_hash,
        contract_address,
        dt,
        data.input.*
    from base
)

select /* REPARTITION(dt) */ *
from final
