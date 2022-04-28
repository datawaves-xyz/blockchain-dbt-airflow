{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['dt'],
        file_format='parquet',
        pre_hook={
            'sql': 'create or replace function foundation_market_upgradeto_calldecodeudf as "io.iftech.sparkudf.hive.Foundation_market_upgradeTo_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.0.jar";'
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
        foundation_market_upgradeto_calldecodeudf(unhex_input, unhex_output, '{"inputs": [{"internalType": "address", "name": "newImplementation", "type": "address"}], "name": "upgradeTo", "outputs": [], "stateMutability": "nonpayable", "type": "function"}', 'upgradeTo') as data
    from {{ ref('stg_ethereum__traces') }}
    where to_address = lower("0xcDA72070E455bb31C7690a170224Ce43623d0B6f")
    and address_hash = abs(hash(lower("0xcDA72070E455bb31C7690a170224Ce43623d0B6f"))) % 10
    and selector = "0x30783336"
    and selector_hash = abs(hash("0x30783336")) % 10

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
