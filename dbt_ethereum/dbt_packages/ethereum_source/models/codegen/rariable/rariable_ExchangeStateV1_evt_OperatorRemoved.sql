{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['dt'],
        file_format='parquet',
        pre_hook={
            'sql': 'create or replace function rariable_exchangestatev1_operatorremoved_eventdecodeudf as "io.iftech.sparkudf.hive.Rariable_ExchangeStateV1_OperatorRemoved_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.0.jar";'
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
        rariable_exchangestatev1_operatorremoved_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "internalType": "address", "name": "account", "type": "address"}], "name": "OperatorRemoved", "type": "event"}', 'OperatorRemoved') as data
    from {{ ref('stg_ethereum__logs') }}
    where address = lower("0xEd1f5F8724Cc185d4e48a71A7Fac64fA5216E4A8")
    and address_hash = abs(hash(lower("0xEd1f5F8724Cc185d4e48a71A7Fac64fA5216E4A8"))) % 10
    and selector = "0x80c0b871b97b595b16a7741c1b06fed0c6f6f558639f18ccbce50724325dc40d"
    and selector_hash = abs(hash("0x80c0b871b97b595b16a7741c1b06fed0c6f6f558639f18ccbce50724325dc40d")) % 10

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
