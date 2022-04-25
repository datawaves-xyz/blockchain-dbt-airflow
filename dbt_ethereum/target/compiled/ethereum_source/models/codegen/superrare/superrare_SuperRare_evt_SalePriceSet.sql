

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        superrare_superrare_salepriceset_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "_tokenId", "type": "uint256"}, {"indexed": true, "name": "_price", "type": "uint256"}], "name": "SalePriceSet", "type": "event"}', 'SalePriceSet') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x41A322b28D0fF354040e2CbC676F0320d8c8850d")
    and address_hash = abs(hash(lower("0x41A322b28D0fF354040e2CbC676F0320d8c8850d"))) % 10
    and selector = "0xe23ea816dce6d7f5c0b85cbd597e7c3b97b2453791152c0b94e5e5c5f314d2f0"
    and selector_hash = abs(hash("0xe23ea816dce6d7f5c0b85cbd597e7c3b97b2453791152c0b94e5e5c5f314d2f0")) % 10

    
),

final as (
    select
        evt_block_number,
        evt_block_time,
        evt_index,
        evt_tx_hash,
        contract_address,
        dt,
        data.input.*
    from base
)

select /* REPARTITION(dt) */ *
from final