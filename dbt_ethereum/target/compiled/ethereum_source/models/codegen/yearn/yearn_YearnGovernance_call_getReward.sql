select /* REPARTITION(dt) */
    status==1 as call_success,
    block_number as call_block_number,
    block_timestamp as call_block_time,
    trace_address as call_trace_address,
    transaction_hash as call_tx_hash,
    to_address as contract_address,
    dt
from ethereum_stg_ethereum.stg_ethereum__traces
where to_address = lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E")
and address_hash = abs(hash(lower("0x3A22dF48d84957F907e67F4313E3D43179040d6E"))) % 10
and selector = "0x30783364313862393132"
and selector_hash = abs(hash("0x30783364313862393132")) % 10

