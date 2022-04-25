

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        foundation_market_upgraded_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "internalType": "address", "name": "implementation", "type": "address"}], "name": "Upgraded", "type": "event"}', 'Upgraded') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0xcDA72070E455bb31C7690a170224Ce43623d0B6f")
    and address_hash = abs(hash(lower("0xcDA72070E455bb31C7690a170224Ce43623d0B6f"))) % 10
    and selector = "0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b"
    and selector_hash = abs(hash("0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b")) % 10

    
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