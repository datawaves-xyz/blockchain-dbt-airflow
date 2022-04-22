from dbt_airflow.build_dbt_dags import build_dbt_dags
from variables import read_vars

build_dbt_dags(
    start_date='2022-04-20',
    manifest_url=read_vars('manifest', var_prefix='dbt_', required=True),
    notification_emails=read_vars('notification_emails')
)
