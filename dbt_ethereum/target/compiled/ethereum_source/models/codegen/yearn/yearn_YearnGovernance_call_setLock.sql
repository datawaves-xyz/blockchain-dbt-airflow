

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        yearn_yearngovernance_setlock_calldecodeudf(unhex_input, unhex_output, '{"constant": false, "inputs": [{"internalType": "uint256", "name": "_lock", "type": "uint256"}], "name": "setLock", "outputs": [], "payable": false, "stateMutability": "nonpayable", "type": "function"}', 'setLock') as data
    from ethereum_stg_ethereum.stg_ethereum__traces
    where to_address = lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E")
    and address_hash = abs(hash(lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E"))) % 10
    and selector = "0x30786433"
    and selector_hash = abs(hash("0x30786433")) % 10

    
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