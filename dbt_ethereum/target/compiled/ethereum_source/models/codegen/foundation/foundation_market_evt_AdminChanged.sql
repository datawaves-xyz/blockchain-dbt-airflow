

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        foundation_market_adminchanged_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": false, "internalType": "address", "name": "previousAdmin", "type": "address"}, {"indexed": false, "internalType": "address", "name": "newAdmin", "type": "address"}], "name": "AdminChanged", "type": "event"}', 'AdminChanged') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0xcDA72070E455bb31C7690a170224Ce43623d0B6f")
    and address_hash = abs(hash(lower("0xcDA72070E455bb31C7690a170224Ce43623d0B6f"))) % 10
    and selector = "0x7e644d79422f17c01e4894b5f4f588d331ebfa28653d42ae832dc59e38c9798f"
    and selector_hash = abs(hash("0x7e644d79422f17c01e4894b5f4f588d331ebfa28653d42ae832dc59e38c9798f")) % 10

    
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