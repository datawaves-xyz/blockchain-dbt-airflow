

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        opensea_wyvernexchangev1_changeminimummakerprotocolfee_calldecodeudf(unhex_input, unhex_output, '{"constant": false, "inputs": [{"name": "newMinimumMakerProtocolFee", "type": "uint256"}], "name": "changeMinimumMakerProtocolFee", "outputs": [], "payable": false, "stateMutability": "nonpayable", "type": "function"}', 'changeMinimumMakerProtocolFee') as data
    from ethereum_stg_ethereum.stg_ethereum__traces
    where to_address = lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b")
    and address_hash = abs(hash(lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b"))) % 10
    and selector = "0x30783134"
    and selector_hash = abs(hash("0x30783134")) % 10

    
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