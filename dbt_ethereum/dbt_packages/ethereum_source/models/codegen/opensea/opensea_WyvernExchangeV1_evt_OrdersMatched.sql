{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['dt'],
        file_format='parquet',
        pre_hook={
            'sql': 'create or replace function opensea_wyvernexchangev1_ordersmatched_eventdecodeudf as "io.iftech.sparkudf.hive.Opensea_WyvernExchangeV1_OrdersMatched_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.0.jar";'
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
        opensea_wyvernexchangev1_ordersmatched_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": false, "name": "buyHash", "type": "bytes32"}, {"indexed": false, "name": "sellHash", "type": "bytes32"}, {"indexed": true, "name": "maker", "type": "address"}, {"indexed": true, "name": "taker", "type": "address"}, {"indexed": false, "name": "price", "type": "uint256"}, {"indexed": true, "name": "metadata", "type": "bytes32"}], "name": "OrdersMatched", "type": "event"}', 'OrdersMatched') as data
    from {{ ref('stg_ethereum__logs') }}
    where address = lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b")
    and address_hash = abs(hash(lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b"))) % 10
    and selector = "0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9"
    and selector_hash = abs(hash("0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9")) % 10

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
