

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        curve_husdswap_get_dy_calldecodeudf(unhex_input, unhex_output, '{"name": "get_dy", "outputs": [{"type": "uint256", "name": ""}], "inputs": [{"type": "int128", "name": "i"}, {"type": "int128", "name": "j"}, {"type": "uint256", "name": "dx"}], "stateMutability": "view", "type": "function", "gas": 2390368}', 'get_dy') as data
    from ethereum_stg_ethereum.stg_ethereum__traces
    where to_address = lower("0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604")
    and address_hash = abs(hash(lower("0x3eF6A01A0f81D6046290f3e2A8c5b843e738E604"))) % 10
    and selector = "0x30783565"
    and selector_hash = abs(hash("0x30783565")) % 10

    
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