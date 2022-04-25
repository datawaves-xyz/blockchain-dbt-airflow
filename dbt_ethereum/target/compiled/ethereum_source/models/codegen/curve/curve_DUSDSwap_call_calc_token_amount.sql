

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        curve_dusdswap_calc_token_amount_calldecodeudf(unhex_input, unhex_output, '{"name": "calc_token_amount", "outputs": [{"type": "uint256", "name": ""}], "inputs": [{"type": "uint256[2]", "name": "amounts"}, {"type": "bool", "name": "is_deposit"}], "stateMutability": "view", "type": "function", "gas": 3938567}', 'calc_token_amount') as data
    from ethereum_stg_ethereum.stg_ethereum__traces
    where to_address = lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c")
    and address_hash = abs(hash(lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c"))) % 10
    and selector = "0x30786564"
    and selector_hash = abs(hash("0x30786564")) % 10

    
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