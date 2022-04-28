

with base as (
    select
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        address as contract_address,
        dt,
        opensea_wyvernexchangev1_orderapprovedpartone_eventdecodeudf(unhex_data, topics_arr, '{"anonymous": false, "inputs": [{"indexed": true, "name": "hash", "type": "bytes32"}, {"indexed": false, "name": "exchange", "type": "address"}, {"indexed": true, "name": "maker", "type": "address"}, {"indexed": false, "name": "taker", "type": "address"}, {"indexed": false, "name": "makerRelayerFee", "type": "uint256"}, {"indexed": false, "name": "takerRelayerFee", "type": "uint256"}, {"indexed": false, "name": "makerProtocolFee", "type": "uint256"}, {"indexed": false, "name": "takerProtocolFee", "type": "uint256"}, {"indexed": true, "name": "feeRecipient", "type": "address"}, {"indexed": false, "name": "feeMethod", "type": "uint8"}, {"indexed": false, "name": "side", "type": "uint8"}, {"indexed": false, "name": "saleKind", "type": "uint8"}, {"indexed": false, "name": "target", "type": "address"}], "name": "OrderApprovedPartOne", "type": "event"}', 'OrderApprovedPartOne') as data
    from ethereum_stg_ethereum.stg_ethereum__logs
    where address = lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b")
    and address_hash = abs(hash(lower("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b"))) % 10
    and selector = "0x90c7f9f5b58c15f0f635bfb99f55d3d78fdbef3559e7d8abf5c81052a5276622"
    and selector_hash = abs(hash("0x90c7f9f5b58c15f0f635bfb99f55d3d78fdbef3559e7d8abf5c81052a5276622")) % 10

    
      and dt = 'not-set'
    
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