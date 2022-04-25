

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        opensea_openseaensresolver_settext_calldecodeudf(unhex_input, unhex_output, '{"constant": false, "inputs": [{"name": "node", "type": "bytes32"}, {"name": "key", "type": "string"}, {"name": "value", "type": "string"}], "name": "setText", "outputs": [], "payable": false, "stateMutability": "nonpayable", "type": "function"}', 'setText') as data
    from ethereum_stg_ethereum.stg_ethereum__traces
    where to_address = lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956")
    and address_hash = abs(hash(lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956"))) % 10
    and selector = "0x30783130"
    and selector_hash = abs(hash("0x30783130")) % 10

    
      and dt = 'not-set'
    
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