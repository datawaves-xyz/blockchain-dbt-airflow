

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        curve_dusdswap_rampa_eventdecodeudf(unhex_data, topics_arr, '{"name": "RampA", "inputs": [{"type": "uint256", "name": "old_A", "indexed": false}, {"type": "uint256", "name": "new_A", "indexed": false}, {"type": "uint256", "name": "initial_time", "indexed": false}, {"type": "uint256", "name": "future_time", "indexed": false}], "anonymous": false, "type": "event"}', 'RampA') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c")
    and address_hash = abs(hash(lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c"))) % 10
    and selector = "0xa2b71ec6df949300b59aab36b55e189697b750119dd349fcfa8c0f779e83c254"
    and selector_hash = abs(hash("0xa2b71ec6df949300b59aab36b55e189697b750119dd349fcfa8c0f779e83c254")) % 10

    
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