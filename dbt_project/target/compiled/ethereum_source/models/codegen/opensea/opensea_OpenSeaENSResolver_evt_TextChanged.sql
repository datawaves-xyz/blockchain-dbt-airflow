

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        opensea_openseaensresolver_textchanged_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "node", "type": "bytes32"}, {"indexed": false, "name": "indexedKey", "type": "string"}, {"indexed": false, "name": "key", "type": "string"}], "name": "TextChanged", "type": "event"}', 'TextChanged') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956")
    and address_hash = abs(hash(lower("0x9c4e9cce4780062942a7fe34fa2fa7316c872956"))) % 10
    and selector = "0xd8c9334b1a9c2f9da342a0a2b32629c1a229b6445dad78947f674b44444a7550"
    and selector_hash = abs(hash("0xd8c9334b1a9c2f9da342a0a2b32629c1a229b6445dad78947f674b44444a7550")) % 10

    
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