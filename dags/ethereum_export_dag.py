from ethereum_airflow.build_export_dag import build_export_dag
from variables import parse_list, read_vars, parse_bool

prefix = 'ethereum_'

build_export_dag_toggle = parse_bool(read_vars('build_export_dag_toggle', prefix, False))

if build_export_dag_toggle:
    DAG = build_export_dag(
        dag_id='ethereum_export_dag',
        provider_uris=parse_list(read_vars('provider_uris', prefix, True), ','),
        provider_uris_archival=parse_list(read_vars('provider_uris_archival', prefix, True), ','),
        output_bucket=read_vars('output_bucket', prefix, True),
        export_start_date='2022-04-25',
        notification_emails=read_vars('notification_emails'),
        export_daofork_traces_option=parse_bool(read_vars('export_daofork_traces_option', prefix, False)),
        export_genesis_traces_option=parse_bool(read_vars('export_genesis_traces_option', prefix, False)),
        export_blocks_and_transactions_toggle=parse_bool(
            read_vars('export_blocks_and_transactions_toggle', prefix, False)),
        export_receipts_and_logs_toggle=parse_bool(read_vars('export_receipts_and_logs_toggle', prefix, False)),
        extract_contracts_toggle=parse_bool(read_vars('extract_contracts_toggle', prefix, False)),
        extract_tokens_toggle=parse_bool(read_vars('extract_tokens_toggle', prefix, False)),
        extract_token_transfers_toggle=parse_bool(read_vars('extract_token_transfers_toggle', prefix, False)),
        export_traces_toggle=parse_bool(read_vars('export_traces_toggle', prefix, False)),
        export_prices_usd_toggle=parse_bool(read_vars('export_prices_usd_toggle', prefix, False))
    )
