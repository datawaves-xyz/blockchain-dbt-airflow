

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        curve_dusdswap_addliquidity_eventdecodeudf(unhex_data, topics_arr, '{"name": "AddLiquidity", "inputs": [{"type": "address", "name": "provider", "indexed": true}, {"type": "uint256[2]", "name": "token_amounts", "indexed": false}, {"type": "uint256[2]", "name": "fees", "indexed": false}, {"type": "uint256", "name": "invariant", "indexed": false}, {"type": "uint256", "name": "token_supply", "indexed": false}], "anonymous": false, "type": "event"}', 'AddLiquidity') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c")
    and address_hash = abs(hash(lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c"))) % 10
    and selector = "0x26f55a85081d24974e85c6c00045d0f0453991e95873f52bff0d21af4079a768"
    and selector_hash = abs(hash("0x26f55a85081d24974e85c6c00045d0f0453991e95873f52bff0d21af4079a768")) % 10

    
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