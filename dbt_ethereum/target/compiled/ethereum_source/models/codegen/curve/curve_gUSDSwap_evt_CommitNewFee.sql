

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        curve_gusdswap_commitnewfee_eventdecodeudf(unhex_data, topics_arr, '{"name": "CommitNewFee", "inputs": [{"type": "uint256", "name": "deadline", "indexed": true}, {"type": "uint256", "name": "fee", "indexed": false}, {"type": "uint256", "name": "admin_fee", "indexed": false}], "anonymous": false, "type": "event"}', 'CommitNewFee') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956")
    and address_hash = abs(hash(lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956"))) % 10
    and selector = "0x351fc5da2fbf480f2225debf3664a4bc90fa9923743aad58b4603f648e931fe0"
    and selector_hash = abs(hash("0x351fc5da2fbf480f2225debf3664a4bc90fa9923743aad58b4603f648e931fe0")) % 10

    
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