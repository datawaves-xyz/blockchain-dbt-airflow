

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        rariable_exchangev2_beaconupgraded_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "internalType": "address", "name": "beacon", "type": "address"}], "name": "BeaconUpgraded", "type": "event"}', 'BeaconUpgraded') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x9757F2d2b135150BBeb65308D4a91804107cd8D6")
    and address_hash = abs(hash(lower("0x9757F2d2b135150BBeb65308D4a91804107cd8D6"))) % 10
    and selector = "0x1cf3b03a6cf19fa2baba4df148e9dcabedea7f8a5c07840e207e5c089be95d3e"
    and selector_hash = abs(hash("0x1cf3b03a6cf19fa2baba4df148e9dcabedea7f8a5c07840e207e5c089be95d3e")) % 10

    
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