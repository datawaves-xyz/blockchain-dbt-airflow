

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        curve_busdswap_newparameters_eventdecodeudf(unhex_data, topics_arr, '{"name": "NewParameters", "inputs": [{"type": "uint256", "name": "A", "indexed": false}, {"type": "uint256", "name": "fee", "indexed": false}, {"type": "uint256", "name": "admin_fee", "indexed": false}], "anonymous": false, "type": "event"}', 'NewParameters') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27")
    and address_hash = abs(hash(lower("0x79a8C46DeA5aDa233ABaFFD40F3A0A2B1e5A4F27"))) % 10
    and selector = "0x752a27d1853eb7af3ee4ff764f2c4a51619386af721573dd3809e929c39db99e"
    and selector_hash = abs(hash("0x752a27d1853eb7af3ee4ff764f2c4a51619386af721573dd3809e929c39db99e")) % 10

    
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