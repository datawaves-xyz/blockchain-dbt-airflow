

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        cryptopunks_cryptopunksmarket_punknolongerforsale_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "punkIndex", "type": "uint256"}], "name": "PunkNoLongerForSale", "type": "event"}', 'PunkNoLongerForSale') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB")
    and address_hash = abs(hash(lower("0xb47e3cd837dDF8e4c57F05d70Ab865de6e193BBB"))) % 10
    and selector = "0xb0e0a660b4e50f26f0b7ce75c24655fc76cc66e3334a54ff410277229fa10bd4"
    and selector_hash = abs(hash("0xb0e0a660b4e50f26f0b7ce75c24655fc76cc66e3334a54ff410277229fa10bd4")) % 10

    
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