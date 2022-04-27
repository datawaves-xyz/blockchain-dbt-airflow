

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        curve_busdswap_calc_token_amount_calldecodeudf(unhex_input, unhex_output, '{"name": "calc_token_amount", "outputs": [{"type": "uint256", "name": "out"}], "inputs": [{"type": "uint256[4]", "name": "amounts"}, {"type": "bool", "name": "deposit"}], "constant": true, "payable": false, "type": "function", "gas": 6067881}', 'calc_token_amount') as data
    from ethereum_stg_ethereum.stg_ethereum__traces
    where to_address = lower("0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27")
    and address_hash = abs(hash(lower("0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27"))) % 10
    and selector = "0x30786366"
    and selector_hash = abs(hash("0x30786366")) % 10

    
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