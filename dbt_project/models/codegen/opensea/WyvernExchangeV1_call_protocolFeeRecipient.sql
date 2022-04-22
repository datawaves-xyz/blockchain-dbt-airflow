{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['dt'],
        file_format='parquet',
        pre_hook={
            'sql': 'create or replace function opensea_wyvernexchangev1_protocolfeerecipient_calldecodeudf as "io.iftech.sparkudf.hive.opensea_WyvernExchangeV1_protocolFeeRecipient_CallDecodeUDF" using jar "s3a://ifcrypto/blockchain-dbt/jars/opensea_udf.jar";'
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
        opensea_wyvernexchangev1_protocolfeerecipient_calldecodeudf(unhex_input, unhex_output, '{"constant": true, "inputs": [], "name": "protocolFeeRecipient", "outputs": [{"name": "", "type": "address"}], "payable": false, "stateMutability": "view", "type": "function"}', 'protocolFeeRecipient') as data
    from {{ ref('stg_ethereum__traces') }}
    where to_address = lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b")
    and address_hash = abs(hash(lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b"))) % 10
    and selector = "0x30783634"
    and selector_hash = abs(hash("0x30783634")) % 10

    {% if is_incremental() %}
      and dt = var('dt')
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
