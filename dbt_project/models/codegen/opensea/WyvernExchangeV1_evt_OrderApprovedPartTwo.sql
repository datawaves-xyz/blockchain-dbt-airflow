{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['dt'],
        file_format='parquet',
        pre_hook={
            'sql': 'create or replace function opensea_wyvernexchangev1_orderapprovedparttwo_eventdecodeudf as "io.iftech.sparkudf.hive.Opensea_WyvernExchangeV1_OrderApprovedPartTwo_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf.jar";'
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
        opensea_wyvernexchangev1_orderapprovedparttwo_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "hash", "type": "bytes32"}, {"indexed": false, "name": "howToCall", "type": "uint8"}, {"indexed": false, "name": "calldata", "type": "bytes"}, {"indexed": false, "name": "replacementPattern", "type": "bytes"}, {"indexed": false, "name": "staticTarget", "type": "address"}, {"indexed": false, "name": "staticExtradata", "type": "bytes"}, {"indexed": false, "name": "paymentToken", "type": "address"}, {"indexed": false, "name": "basePrice", "type": "uint256"}, {"indexed": false, "name": "extra", "type": "uint256"}, {"indexed": false, "name": "listingTime", "type": "uint256"}, {"indexed": false, "name": "expirationTime", "type": "uint256"}, {"indexed": false, "name": "salt", "type": "uint256"}, {"indexed": false, "name": "orderbookInclusionDesired", "type": "bool"}], "name": "OrderApprovedPartTwo", "type": "event"}', 'OrderApprovedPartTwo') as data
    from {{ ref('stg_ethereum__logs') }}
    where address = lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b")
    and address_hash = abs(hash(lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b"))) % 10
    and selector = "0xe55393c778364e440d958b39ac1debd99dcfae3775a8a04d1e79124adf6a2d08"
    and selector_hash = abs(hash("0xe55393c778364e440d958b39ac1debd99dcfae3775a8a04d1e79124adf6a2d08")) % 10

    {% if is_incremental() %}
      and dt = var('dt')
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
