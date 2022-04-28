{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['dt'],
        file_format='parquet',
        pre_hook={
            'sql': 'create or replace function superrare_superrare_whitelistcreator_eventdecodeudf as "io.iftech.sparkudf.hive.Superrare_SuperRare_WhitelistCreator_EventDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.0.jar";'
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
        superrare_superrare_whitelistcreator_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "_creator", "type": "address"}], "name": "WhitelistCreator", "type": "event"}', 'WhitelistCreator') as data
    from {{ ref('stg_ethereum__logs') }}
    where address = lower("0x41A322b28D0fF354040e2CbC676F0320d8c8850d")
    and address_hash = abs(hash(lower("0x41A322b28D0fF354040e2CbC676F0320d8c8850d"))) % 10
    and selector = "0x55eed0aed3ec6e015b9ad5e984675fe36c0ce3aebdcb70f467670773f19f7f8d"
    and selector_hash = abs(hash("0x55eed0aed3ec6e015b9ad5e984675fe36c0ce3aebdcb70f467670773f19f7f8d")) % 10

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
