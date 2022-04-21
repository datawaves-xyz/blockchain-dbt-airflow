from dags.dbt_airflow.build_dbt_dag import DbtDagsBuilder
from dags.variables import read_vars

builder = DbtDagsBuilder(
    start_date='2022-06-13',
    notification_emails=read_vars('notification_emails')
)

builder.build_dbt_dags(
    repo_url=read_vars('repo_url', var_prefix='dbt_', required=True),
    repo_tag=read_vars('repo_tag', var_prefix='dbt_', default='master'),
    workspace='/tmp/dbt',
    profiles_dir='/tmp/profiles'
)
