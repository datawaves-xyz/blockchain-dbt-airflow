

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        opensea_wyvernexchangev1_inverse_basis_point_calldecodeudf(unhex_input, unhex_output, '{"constant": true, "inputs": [], "name": "INVERSE_BASIS_POINT", "outputs": [{"name": "", "type": "uint256"}], "payable": false, "stateMutability": "view", "type": "function"}', 'INVERSE_BASIS_POINT') as data
    from ethereum_stg_ethereum.stg_ethereum__traces
    where to_address = lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b")
    and address_hash = abs(hash(lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b"))) % 10
    and selector = "0x30786361"
    and selector_hash = abs(hash("0x30786361")) % 10

    
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