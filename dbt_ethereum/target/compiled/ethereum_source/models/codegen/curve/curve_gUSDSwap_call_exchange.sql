

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        curve_gusdswap_exchange_calldecodeudf(unhex_input, unhex_output, '{"name": "exchange", "outputs": [{"type": "uint256", "name": ""}], "inputs": [{"type": "int128", "name": "i"}, {"type": "int128", "name": "j"}, {"type": "uint256", "name": "dx"}, {"type": "uint256", "name": "min_dy"}], "stateMutability": "nonpayable", "type": "function", "gas": 2617568}', 'exchange') as data
    from ethereum_stg_ethereum.stg_ethereum__traces
    where to_address = lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956")
    and address_hash = abs(hash(lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956"))) % 10
    and selector = "0x30783364"
    and selector_hash = abs(hash("0x30783364")) % 10

    
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