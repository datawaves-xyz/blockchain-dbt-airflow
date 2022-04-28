{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['dt'],
        file_format='parquet',
        pre_hook={
            'sql': 'create or replace function rariable_exchangev1_cancel_eventdecodeudf as "io.iftech.sparkudf.hive.Rariable_ExchangeV1_Cancel_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.0.jar";'
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
        rariable_exchangev1_cancel_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "internalType": "address", "name": "sellToken", "type": "address"}, {"indexed": true, "internalType": "uint256", "name": "sellTokenId", "type": "uint256"}, {"indexed": false, "internalType": "address", "name": "owner", "type": "address"}, {"indexed": false, "internalType": "address", "name": "buyToken", "type": "address"}, {"indexed": false, "internalType": "uint256", "name": "buyTokenId", "type": "uint256"}, {"indexed": false, "internalType": "uint256", "name": "salt", "type": "uint256"}], "name": "Cancel", "type": "event"}', 'Cancel') as data
    from {{ ref('stg_ethereum__logs') }}
    where address = lower("0xcd4EC7b66fbc029C116BA9Ffb3e59351c20B5B06")
    and address_hash = abs(hash(lower("0xcd4EC7b66fbc029C116BA9Ffb3e59351c20B5B06"))) % 10
    and selector = "0xbfe0e802e586c99960de1a111c80f598b281996d65080d74dbe29986f55b274a"
    and selector_hash = abs(hash("0xbfe0e802e586c99960de1a111c80f598b281996d65080d74dbe29986f55b274a")) % 10

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
