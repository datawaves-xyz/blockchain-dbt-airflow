

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        rariable_exchangev1_setbeneficiary_calldecodeudf(unhex_input, unhex_output, '{"constant": false, "inputs": [{"internalType": "address payable", "name": "newBeneficiary", "type": "address"}], "name": "setBeneficiary", "outputs": [], "payable": false, "stateMutability": "nonpayable", "type": "function"}', 'setBeneficiary') as data
    from ethereum_stg_ethereum.stg_ethereum__traces
    where to_address = lower("0xcd4EC7b66fbc029C116BA9Ffb3e59351c20B5B06")
    and address_hash = abs(hash(lower("0xcd4EC7b66fbc029C116BA9Ffb3e59351c20B5B06"))) % 10
    and selector = "0x30783163"
    and selector_hash = abs(hash("0x30783163")) % 10

    
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