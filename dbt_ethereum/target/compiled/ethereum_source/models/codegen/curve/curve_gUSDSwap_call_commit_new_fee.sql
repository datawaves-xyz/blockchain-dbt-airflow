

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        curve_gusdswap_commit_new_fee_calldecodeudf(unhex_input, unhex_output, '{"name": "commit_new_fee", "outputs": [], "inputs": [{"type": "uint256", "name": "new_fee"}, {"type": "uint256", "name": "new_admin_fee"}], "stateMutability": "nonpayable", "type": "function", "gas": 110491}', 'commit_new_fee') as data
    from ethereum_stg_ethereum.stg_ethereum__traces
    where to_address = lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956")
    and address_hash = abs(hash(lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956"))) % 10
    and selector = "0x30783562"
    and selector_hash = abs(hash("0x30783562")) % 10

    
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