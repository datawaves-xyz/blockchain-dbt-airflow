

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        looksrare_looksrareexchange_takerbid_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": false, "internalType": "bytes32", "name": "orderHash", "type": "bytes32"}, {"indexed": false, "internalType": "uint256", "name": "orderNonce", "type": "uint256"}, {"indexed": true, "internalType": "address", "name": "taker", "type": "address"}, {"indexed": true, "internalType": "address", "name": "maker", "type": "address"}, {"indexed": true, "internalType": "address", "name": "strategy", "type": "address"}, {"indexed": false, "internalType": "address", "name": "currency", "type": "address"}, {"indexed": false, "internalType": "address", "name": "collection", "type": "address"}, {"indexed": false, "internalType": "uint256", "name": "tokenId", "type": "uint256"}, {"indexed": false, "internalType": "uint256", "name": "amount", "type": "uint256"}, {"indexed": false, "internalType": "uint256", "name": "price", "type": "uint256"}], "name": "TakerBid", "type": "event"}', 'TakerBid') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a")
    and address_hash = abs(hash(lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a"))) % 10
    and selector = "0x95fb6205e23ff6bda16a2d1dba56b9ad7c783f67c96fa149785052f47696f2be"
    and selector_hash = abs(hash("0x95fb6205e23ff6bda16a2d1dba56b9ad7c783f67c96fa149785052f47696f2be")) % 10

    
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