

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        looksrare_looksrareexchange_royaltypayment_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "internalType": "address", "name": "collection", "type": "address"}, {"indexed": true, "internalType": "uint256", "name": "tokenId", "type": "uint256"}, {"indexed": true, "internalType": "address", "name": "royaltyRecipient", "type": "address"}, {"indexed": false, "internalType": "address", "name": "currency", "type": "address"}, {"indexed": false, "internalType": "uint256", "name": "amount", "type": "uint256"}], "name": "RoyaltyPayment", "type": "event"}', 'RoyaltyPayment') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a")
    and address_hash = abs(hash(lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a"))) % 10
    and selector = "0x27c4f0403323142b599832f26acd21c74a9e5b809f2215726e244a4ac588cd7d"
    and selector_hash = abs(hash("0x27c4f0403323142b599832f26acd21c74a9e5b809f2215726e244a4ac588cd7d")) % 10

    
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