

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        opensea_wyvernexchangev2_ordercancelled_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "hash", "type": "bytes32"}], "name": "OrderCancelled", "type": "event"}', 'OrderCancelled') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x7f268357A8c2552623316e2562D90e642bB538E5")
    and address_hash = abs(hash(lower("0x7f268357A8c2552623316e2562D90e642bB538E5"))) % 10
    and selector = "0x5152abf959f6564662358c2e52b702259b78bac5ee7842a0f01937e670efcc7d"
    and selector_hash = abs(hash("0x5152abf959f6564662358c2e52b702259b78bac5ee7842a0f01937e670efcc7d")) % 10

    
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