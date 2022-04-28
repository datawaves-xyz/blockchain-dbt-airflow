{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['dt'],
        file_format='parquet',
        pre_hook={
            'sql': 'create or replace function looksrare_looksrareexchange_newprotocolfeerecipient_eventdecodeudf as "io.iftech.sparkudf.hive.Looksrare_LooksRareExchange_NewProtocolFeeRecipient_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.0.jar";'
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
        looksrare_looksrareexchange_newprotocolfeerecipient_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "internalType": "address", "name": "protocolFeeRecipient", "type": "address"}], "name": "NewProtocolFeeRecipient", "type": "event"}', 'NewProtocolFeeRecipient') as data
    from {{ ref('stg_ethereum__logs') }}
    where address = lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a")
    and address_hash = abs(hash(lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a"))) % 10
    and selector = "0x8cffb07faa2874440346743bdc0a86b06c3335cc47dc49b327d10e77b73ceb10"
    and selector_hash = abs(hash("0x8cffb07faa2874440346743bdc0a86b06c3335cc47dc49b327d10e77b73ceb10")) % 10

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
