

with base as (
    select
        status==1 as call_success,
        block_number as call_block_number,
        block_timestamp as call_block_time,
        trace_address as call_trace_address,
        transaction_hash as call_tx_hash,
        to_address as contract_address,
        dt,
        looksrare_looksrareexchange_transferselectornft_calldecodeudf(unhex_input, unhex_output, '{"inputs": [], "name": "transferSelectorNFT", "outputs": [{"internalType": "contract ITransferSelectorNFT", "name": "", "type": "address"}], "stateMutability": "view", "type": "function"}', 'transferSelectorNFT') as data
    from ethereum_stg_ethereum.stg_ethereum__traces
    where to_address = lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a")
    and address_hash = abs(hash(lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a"))) % 10
    and selector = "0x30783565"
    and selector_hash = abs(hash("0x30783565")) % 10

    
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