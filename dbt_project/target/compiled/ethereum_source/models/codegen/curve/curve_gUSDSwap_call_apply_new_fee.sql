

select /* REPARTITION(dt) */
    status==1 as call_success,
    block_number as call_block_number,
    block_timestamp as call_block_time,
    trace_address as call_trace_address,
    transaction_hash as call_tx_hash,
    to_address as contract_address,
    dt
from ethereum_stg_ethereum.stg_ethereum__traces
where to_address = lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956")
and address_hash = abs(hash(lower("0x4f062658EaAF2C1ccf8C8e36D6824CDf41167956"))) % 10
and selector = "0x30783466313266653937"
and selector_hash = abs(hash("0x30783466313266653937")) % 10

