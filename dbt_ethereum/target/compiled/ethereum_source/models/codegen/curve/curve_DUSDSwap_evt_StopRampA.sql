

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        curve_dusdswap_stoprampa_eventdecodeudf(unhex_data, topics_arr, '{"name": "StopRampA", "inputs": [{"type": "uint256", "name": "A", "indexed": false}, {"type": "uint256", "name": "t", "indexed": false}], "anonymous": false, "type": "event"}', 'StopRampA') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c")
    and address_hash = abs(hash(lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c"))) % 10
    and selector = "0x46e22fb3709ad289f62ce63d469248536dbc78d82b84a3d7e74ad606dc201938"
    and selector_hash = abs(hash("0x46e22fb3709ad289f62ce63d469248536dbc78d82b84a3d7e74ad606dc201938")) % 10

    
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