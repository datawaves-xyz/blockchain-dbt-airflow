

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        rariable_exchangev2_upgradetoandcall_calldecodeudf(unhex_input, unhex_output, '{"inputs": [{"internalType": "address", "name": "newImplementation", "type": "address"}, {"internalType": "bytes", "name": "data", "type": "bytes"}], "name": "upgradeToAndCall", "outputs": [], "stateMutability": "payable", "type": "function"}', 'upgradeToAndCall') as data
    from ethereum_stg_ethereum.stg_ethereum__traces
    where to_address = lower("0x9757F2d2b135150BBeb65308D4a91804107cd8D6")
    and address_hash = abs(hash(lower("0x9757F2d2b135150BBeb65308D4a91804107cd8D6"))) % 10
    and selector = "0x30783466"
    and selector_hash = abs(hash("0x30783466")) % 10

    
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