import logging
import os
from datetime import timedelta
from tempfile import TemporaryDirectory
from typing import List, Callable, Tuple, Optional

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from ethereumetl.cli import (
    export_blocks_and_transactions,
    get_block_range_for_date,
    extract_field,
    export_receipts_and_logs, extract_contracts, extract_tokens, extract_token_transfers, export_traces
)
from pendulum import datetime

from ethereum_airflow.price import CoinpaprikaPriceProvider
from ethereum_airflow.token import DuneTokenProvider


def build_export_dag(
        dag_id: str,
        provider_uris: List[str],
        provider_uris_archival: List[str],
        output_bucket: str,
        export_start_date: str,
        export_max_workers=10,
        export_batch_size=10,
        notification_emails=None,
        export_schedule_interval='0 0 * * *',
        **kwargs
) -> DAG:
    default_dag_args = {
        'depends_on_past': False,
        'start_date': export_start_date,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    # Get all toggles
    export_daofork_traces_option = kwargs.get('export_daofork_traces_option')
    export_genesis_traces_option = kwargs.get('export_genesis_traces_option')
    export_blocks_and_transactions_toggle = kwargs.get('export_blocks_and_transactions_toggle')
    export_receipts_and_logs_toggle = kwargs.get('export_receipts_and_logs_toggle')
    extract_contracts_toggle = kwargs.get('extract_contracts_toggle')
    extract_tokens_toggle = kwargs.get('extract_tokens_toggle')
    extract_token_transfers_toggle = kwargs.get('extract_token_transfers_toggle')
    export_traces_toggle = kwargs.get('export_traces_toggle')
    export_prices_usd_toggle = kwargs.get('export_prices_usd_toggle')

    dag = DAG(
        dag_id,
        catchup=False,
        schedule_interval=export_schedule_interval,
        default_args=default_dag_args,
    )

    hook = S3Hook(aws_conn_id='aws_default')

    # Inner tool functions
    def copy_to_export_path(
            file_path: str, export_path: str,
    ) -> None:
        logging.info('Calling copy_to_export_path({}, {})'.format(file_path, export_path))
        filename = os.path.basename(file_path)
        hook.load_file(
            filename=filename,
            bucket_name=output_bucket,
            key=export_path + filename,
            replace=True,
            encrypt=False
        )

    def copy_from_export_path(
            export_path: str, file_path: str,
    ) -> None:
        logging.info('Calling copy_from_export_path({}, {})'.format(export_path, file_path))
        filename = os.path.basename(file_path)
        hook.download_file(
            bucket=output_bucket,
            key=export_path + filename,
            local_path=file_path
        )

    # Python callable wrapper
    def export_blocks_and_transactions_command(
            execution_date: datetime, provider_uri: str, **kwargs
    ) -> None:
        with TemporaryDirectory() as tempdir:
            start_block, end_block = get_block_range(tempdir, execution_date, provider_uri)

            logging.info('Calling export_blocks_and_transactions({}, {}, {}, {}, {}, ...)'.format(
                start_block, end_block, export_batch_size, provider_uri, export_max_workers))

            export_blocks_and_transactions.callback(
                start_block=start_block,
                end_block=end_block,
                batch_size=export_batch_size,
                provider_uri=provider_uri,
                max_workers=export_max_workers,
                blocks_output=os.path.join(tempdir, "blocks.json"),
                transactions_output=os.path.join(tempdir, "transactions.json"),
            )

            copy_to_export_path(
                os.path.join(tempdir, "blocks_meta.txt"), get_export_path("blocks_meta", execution_date))
            copy_to_export_path(
                os.path.join(tempdir, "blocks.json"), get_export_path("blocks", execution_date))
            copy_to_export_path(
                os.path.join(tempdir, "transactions.json"), get_export_path("transactions", execution_date))

    def export_receipts_and_logs_command(
            execution_date: datetime, provider_uri: str, **kwargs
    ) -> None:
        with TemporaryDirectory() as tempdir:
            copy_from_export_path(
                get_export_path("transactions", execution_date), os.path.join(tempdir, "transactions.json"))

            logging.info('Calling extract_csv_column(...)')
            extract_field.callback(
                input=os.path.join(tempdir, "transactions.json"),
                output=os.path.join(tempdir, "transaction_hashes.txt"),
                field="hash",
            )

            logging.info('Calling export_receipts_and_logs({}, ..., {}, {}, ...)'.format(
                export_batch_size, provider_uri, export_max_workers))
            export_receipts_and_logs.callback(
                batch_size=export_batch_size,
                transaction_hashes=os.path.join(tempdir, "transaction_hashes.txt"),
                provider_uri=provider_uri,
                max_workers=export_max_workers,
                receipts_output=os.path.join(tempdir, "receipts.json"),
                logs_output=os.path.join(tempdir, "logs.json"),
            )

            copy_to_export_path(
                os.path.join(tempdir, "receipts.json"), get_export_path("receipts", execution_date))
            copy_to_export_path(
                os.path.join(tempdir, "logs.json"), get_export_path("logs", execution_date))

    def extract_contracts_command(
            execution_date: datetime, **kwargs
    ) -> None:
        with TemporaryDirectory() as tempdir:
            copy_from_export_path(
                get_export_path("traces", execution_date), os.path.join(tempdir, "traces.json"))

            logging.info('Calling extract_contracts(..., {}, {})'.format(
                export_batch_size, export_max_workers
            ))
            extract_contracts.callback(
                traces=os.path.join(tempdir, "traces.json"),
                output=os.path.join(tempdir, "contracts.json"),
                batch_size=export_batch_size,
                max_workers=export_max_workers,
            )

            copy_to_export_path(
                os.path.join(tempdir, "contracts.json"), get_export_path("contracts", execution_date))

    def extract_tokens_command(
            execution_date: datetime, provider_uri: str, **kwargs
    ) -> None:
        with TemporaryDirectory() as tempdir:
            copy_from_export_path(
                get_export_path("contracts", execution_date), os.path.join(tempdir, "contracts.json"))

            logging.info('Calling extract_tokens(..., {}, {})'.format(export_max_workers, provider_uri))
            extract_tokens.callback(
                contracts=os.path.join(tempdir, "contracts.json"),
                output=os.path.join(tempdir, "tokens.json"),
                max_workers=export_max_workers,
                provider_uri=provider_uri,
                values_as_strings=True,
            )

            copy_to_export_path(
                os.path.join(tempdir, "tokens.json"), get_export_path("tokens", execution_date))

    def extract_token_transfers_command(
            execution_date: datetime, **kwargs
    ) -> None:
        with TemporaryDirectory() as tempdir:
            copy_from_export_path(
                get_export_path("logs", execution_date), os.path.join(tempdir, "logs.json")
            )

            logging.info('Calling extract_token_transfers(..., {}, ..., {})'.format(
                export_batch_size, export_max_workers
            ))
            extract_token_transfers.callback(
                logs=os.path.join(tempdir, "logs.json"),
                batch_size=export_batch_size,
                output=os.path.join(tempdir, "token_transfers.json"),
                max_workers=export_max_workers,
                values_as_strings=True,
            )

            copy_to_export_path(
                os.path.join(tempdir, "token_transfers.json"), get_export_path("token_transfers", execution_date))

    def export_traces_command(
            execution_date: datetime, provider_uri: str, **kwargs
    ) -> None:
        with TemporaryDirectory() as tempdir:
            start_block, end_block = get_block_range(tempdir, execution_date, provider_uri)

            logging.info('Calling export_traces({}, {}, {}, ...,{}, {}, {}, {})'.format(
                start_block, end_block, export_batch_size, export_max_workers, provider_uri,
                export_genesis_traces_option, export_daofork_traces_option
            ))
            export_traces.callback(
                start_block=start_block,
                end_block=end_block,
                batch_size=export_batch_size,
                output=os.path.join(tempdir, "traces.json"),
                max_workers=export_max_workers,
                provider_uri=provider_uri,
                genesis_traces=export_genesis_traces_option,
                daofork_traces=export_daofork_traces_option,
            )

            copy_to_export_path(
                os.path.join(tempdir, "traces.json"), get_export_path("traces", execution_date))

    def export_prices_callable(symbol: str) -> Callable:
        def export_prices(execution_date: datetime, **kwargs):
            with TemporaryDirectory() as tempdir:
                start_ts = int(execution_date.start_of('day').timestamp())
                end_ts = int(execution_date.end_of('day').timestamp())

                logging.info('Calling export_prices({}, {})'.format(
                    start_ts, end_ts
                ))
                prices_provider = CoinpaprikaPriceProvider(DuneTokenProvider())
                prices_provider.create_temp_csv(
                    output_path=os.path.join(tempdir, f"prices_{symbol}.csv"),
                    start=start_ts,
                    end=end_ts
                )

                copy_to_export_path(
                    os.path.join(tempdir, f"prices_{symbol}.csv"), get_export_path(f"prices_{symbol}", execution_date))

        return export_prices

    # Operators
    export_blocks_and_transactions_operator = _add_export_task(
        toggle=export_blocks_and_transactions_toggle,
        task_id="export_blocks_and_transactions",
        python_callable=_add_provider_uri_fallback_loop(export_blocks_and_transactions_command, provider_uris),
        dag=dag
    )

    export_receipts_and_logs_operator = _add_export_task(
        toggle=export_receipts_and_logs_toggle,
        task_id="export_receipts_and_logs",
        python_callable=_add_provider_uri_fallback_loop(export_receipts_and_logs_command, provider_uris),
        dag=dag,
        dependencies=[export_blocks_and_transactions_operator],
    )

    _add_export_task(
        toggle=extract_token_transfers_toggle,
        task_id="extract_token_transfers",
        python_callable=extract_token_transfers_command,
        dag=dag,
        dependencies=[export_receipts_and_logs_operator],
    )

    export_traces_operator = _add_export_task(
        toggle=export_traces_toggle,
        task_id="export_traces",
        python_callable=_add_provider_uri_fallback_loop(export_traces_command, provider_uris_archival),
        dag=dag
    )

    extract_contracts_operator = _add_export_task(
        toggle=extract_contracts_toggle,
        task_id="extract_contracts",
        python_callable=extract_contracts_command,
        dag=dag,
        dependencies=[export_traces_operator],
    )

    _add_export_task(
        toggle=extract_tokens_toggle,
        task_id="extract_tokens",
        python_callable=_add_provider_uri_fallback_loop(extract_tokens_command, provider_uris),
        dag=dag,
        dependencies=[extract_contracts_operator],
    )

    _add_export_task(
        toggle=export_prices_usd_toggle,
        task_id="export_prices_usd",
        python_callable=export_prices_callable("usd"),
        dag=dag
    )

    return dag


def get_export_path(
        directory: str, date: datetime
) -> str:
    return f'export/{directory}/block_date={date.strftime("%Y-%m-%d")}'


def get_block_range(
        tempdir: str, date: datetime, provider_uri: str
) -> Tuple[int, int]:
    logging.info('Calling get_block_range_for_date({}, {}, ...)'.format(provider_uri, date))
    get_block_range_for_date.callback(
        provider_uri=provider_uri, date=date, output=os.path.join(tempdir, "blocks_meta.txt")
    )

    with open(os.path.join(tempdir, "blocks_meta.txt")) as block_range_file:
        block_range = block_range_file.read()
        start_block, end_block = block_range.split(",")

    return int(start_block), int(end_block)


def _add_export_task(
        toggle: bool,
        task_id: str,
        python_callable: Callable,
        dag: DAG,
        dependencies: Optional[List[BaseOperator]] = None
):
    if toggle:
        operator = PythonOperator(
            task_id=task_id,
            python_callable=python_callable,
            provide_context=True,
            execution_timeout=timedelta(hours=15),
            dag=dag,
        )
        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                if dependency is not None:
                    dependency >> operator
        return operator
    else:
        return None


def _add_provider_uri_fallback_loop(
        python_callable: Callable, provider_uris: List[str]
) -> Callable:
    """Tries each provider uri in provider_uris until the command succeeds"""

    def python_callable_with_fallback(**kwargs):
        for index, provider_uri in enumerate(provider_uris):
            kwargs['provider_uri'] = provider_uri
            try:
                python_callable(**kwargs)
                break
            except Exception as e:
                if index < (len(provider_uris) - 1):
                    logging.exception('An exception occurred. Trying another uri')
                else:
                    raise e

    return python_callable_with_fallback
