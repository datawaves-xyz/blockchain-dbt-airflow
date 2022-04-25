

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        rariable_exchangestatev1_operatoradded_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "internalType": "address", "name": "account", "type": "address"}], "name": "OperatorAdded", "type": "event"}', 'OperatorAdded') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0xEd1f5F8724Cc185d4e48a71A7Fac64fA5216E4A8")
    and address_hash = abs(hash(lower("0xEd1f5F8724Cc185d4e48a71A7Fac64fA5216E4A8"))) % 10
    and selector = "0xac6fa858e9350a46cec16539926e0fde25b7629f84b5a72bffaae4df888ae86d"
    and selector_hash = abs(hash("0xac6fa858e9350a46cec16539926e0fde25b7629f84b5a72bffaae4df888ae86d")) % 10

    
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