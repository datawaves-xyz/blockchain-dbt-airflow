

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        opensea_openseaensresolver_abichanged_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "node", "type": "bytes32"}, {"indexed": true, "name": "contentType", "type": "uint256"}], "name": "ABIChanged", "type": "event"}', 'ABIChanged') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956")
    and address_hash = abs(hash(lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956"))) % 10
    and selector = "0xaa121bbeef5f32f5961a2a28966e769023910fc9479059ee3495d4c1a696efe3"
    and selector_hash = abs(hash("0xaa121bbeef5f32f5961a2a28966e769023910fc9479059ee3495d4c1a696efe3")) % 10

    
      and dt = 'not-set'
    
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