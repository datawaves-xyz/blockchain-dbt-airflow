

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        rariable_exchangestatev1_transferownership_calldecodeudf(unhex_input, unhex_output, '{"constant": false, "inputs": [{"internalType": "address", "name": "newOwner", "type": "address"}], "name": "transferOwnership", "outputs": [], "payable": false, "stateMutability": "nonpayable", "type": "function"}', 'transferOwnership') as data
    from ethereum_stg_ethereum.stg_ethereum__traces
    where to_address = lower("0xEd1f5F8724Cc185d4e48a71A7Fac64fA5216E4A8")
    and address_hash = abs(hash(lower("0xEd1f5F8724Cc185d4e48a71A7Fac64fA5216E4A8"))) % 10
    and selector = "0x30786632"
    and selector_hash = abs(hash("0x30786632")) % 10

    
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