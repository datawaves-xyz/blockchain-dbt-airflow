

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        curve_dusdswap_commitnewadmin_eventdecodeudf(unhex_data, topics_arr, '{"name": "CommitNewAdmin", "inputs": [{"type": "uint256", "name": "deadline", "indexed": true}, {"type": "address", "name": "admin", "indexed": true}], "anonymous": false, "type": "event"}', 'CommitNewAdmin') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c")
    and address_hash = abs(hash(lower("0x8038C01A0390a8c547446a0b2c18fc9aEFEcc10c"))) % 10
    and selector = "0x181aa3aa17d4cbf99265dd4443eba009433d3cde79d60164fde1d1a192beb935"
    and selector_hash = abs(hash("0x181aa3aa17d4cbf99265dd4443eba009433d3cde79d60164fde1d1a192beb935")) % 10

    
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