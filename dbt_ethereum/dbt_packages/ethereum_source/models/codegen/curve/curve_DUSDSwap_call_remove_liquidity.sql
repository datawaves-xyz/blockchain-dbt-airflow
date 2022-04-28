{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['dt'],
        file_format='parquet',
        pre_hook={
            'sql': 'create or replace function curve_dusdswap_remove_liquidity_calldecodeudf as "io.iftech.sparkudf.hive.Curve_DUSDSwap_remove_liquidity_CallDecodeUDF" using jar "s3a://blockchain-dbt/dist/jars/blockchain-dbt-udf-0.1.0.jar";'
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
        curve_dusdswap_remove_liquidity_calldecodeudf(unhex_input, unhex_output, '{"name": "remove_liquidity", "outputs": [{"type": "uint256[2]", "name": ""}], "inputs": [{"type": "uint256", "name": "_amount"}, {"type": "uint256[2]", "name": "min_amounts"}], "stateMutability": "nonpayable", "type": "function", "gas": 163158}', 'remove_liquidity') as data
    from {{ ref('stg_ethereum__traces') }}
    where to_address = lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c")
    and address_hash = abs(hash(lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c"))) % 10
    and selector = "0x30783562"
    and selector_hash = abs(hash("0x30783562")) % 10

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
