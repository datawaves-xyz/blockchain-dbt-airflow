import logging
import os
from datetime import timedelta, datetime
from tempfile import TemporaryDirectory
from typing import List, Callable, Tuple

from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from ethereumetl.cli import (
    export_blocks_and_transactions,
    get_block_range_for_date,
    extract_field,
    export_receipts_and_logs
)


def build_export_dag(
        dag_id: str,
        provider_uris: str,
        provider_uris_archival: str,
        output_bucket: str,
        export_start_date: str,
        export_max_workers=10,
        export_batch_size=10,
        notification_emails=None,
        export_schedule_interval='0 0 * * *',
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

    dag = DAG(
        dag_id,
        catchup=False,
        schedule_interval=export_schedule_interval,
        default_args=default_dag_args,
    )

    hook = S3Hook(aws_conn_id='aws_default')

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

    def export_blocks_and_transactions_callable(
            execution_date: datetime, provider_uri: str, **kwargs
    ) -> None:
        with TemporaryDirectory() as tempdir:
            start_block, end_block = _get_block_range(tempdir, execution_date, provider_uri)

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
                os.path.join(tempdir, "blocks_meta.txt"), _get_export_path("blocks_meta", execution_date))
            copy_to_export_path(
                os.path.join(tempdir, "blocks.json"), _get_export_path("blocks", execution_date))
            copy_to_export_path(
                os.path.join(tempdir, "transactions.json"), _get_export_path("transactions", execution_date))

    def export_receipts_and_logs_callable(
            execution_date: datetime, provider_uri: str, **kwargs
    ) -> None:
        with TemporaryDirectory() as tempdir:
            copy_from_export_path(
                _get_export_path("transactions", execution_date), os.path.join(tempdir, "transactions.json"))

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
                os.path.join(tempdir, "receipts.json"), _get_export_path("receipts", execution_date))
            copy_to_export_path(
                os.path.join(tempdir, "logs.json"), _get_export_path("logs", execution_date))


def _get_export_path(
        directory: str, date: datetime
) -> str:
    return f'export/{directory}/block_date={date.strftime("%Y-%m-%d")}'


def _get_block_range(
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
