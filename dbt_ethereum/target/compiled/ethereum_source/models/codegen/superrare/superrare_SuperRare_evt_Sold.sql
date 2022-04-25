

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        superrare_superrare_sold_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "_buyer", "type": "address"}, {"indexed": true, "name": "_seller", "type": "address"}, {"indexed": false, "name": "_amount", "type": "uint256"}, {"indexed": true, "name": "_tokenId", "type": "uint256"}], "name": "Sold", "type": "event"}', 'Sold') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x41A322b28D0fF354040e2CbC676F0320d8c8850d")
    and address_hash = abs(hash(lower("0x41A322b28D0fF354040e2CbC676F0320d8c8850d"))) % 10
    and selector = "0x16dd16959a056953a63cf14bf427881e762e54f03d86b864efea8238dd3b822f"
    and selector_hash = abs(hash("0x16dd16959a056953a63cf14bf427881e762e54f03d86b864efea8238dd3b822f")) % 10

    
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