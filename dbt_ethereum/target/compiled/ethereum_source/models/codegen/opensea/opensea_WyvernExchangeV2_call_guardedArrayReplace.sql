

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        opensea_wyvernexchangev2_guardedarrayreplace_calldecodeudf(unhex_input, unhex_output, '{"constant": true, "inputs": [{"name": "array", "type": "bytes"}, {"name": "desired", "type": "bytes"}, {"name": "mask", "type": "bytes"}], "name": "guardedArrayReplace", "outputs": [{"name": "", "type": "bytes"}], "payable": false, "stateMutability": "pure", "type": "function"}', 'guardedArrayReplace') as data
    from ethereum_stg_ethereum.stg_ethereum__traces
    where to_address = lower("0x7f268357A8c2552623316e2562D90e642bB538E5")
    and address_hash = abs(hash(lower("0x7f268357A8c2552623316e2562D90e642bB538E5"))) % 10
    and selector = "0x30783233"
    and selector_hash = abs(hash("0x30783233")) % 10

    
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