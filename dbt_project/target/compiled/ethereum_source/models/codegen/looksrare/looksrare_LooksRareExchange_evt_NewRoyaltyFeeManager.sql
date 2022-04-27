

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        looksrare_looksrareexchange_newroyaltyfeemanager_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "internalType": "address", "name": "royaltyFeeManager", "type": "address"}], "name": "NewRoyaltyFeeManager", "type": "event"}', 'NewRoyaltyFeeManager') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a")
    and address_hash = abs(hash(lower("0x59728544B08AB483533076417FbBB2fD0B17CE3a"))) % 10
    and selector = "0x80e3874461ebbd918ac3e81da0a92e5e51387d70f337237c9123e48d20e5a508"
    and selector_hash = abs(hash("0x80e3874461ebbd918ac3e81da0a92e5e51387d70f337237c9123e48d20e5a508")) % 10

    
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