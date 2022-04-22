from dbt_airflow.build_dbt_dags import build_dbt_dags
from variables import read_vars, parse_dict

prefix = 'dbt_'

dag_map = build_dbt_dags(
    start_date='2022-04-20',
    manifest_url=read_vars('manifest', var_prefix=prefix, required=True),
    workspace='/tmp',
    repo_url=read_vars('repo_url', var_prefix=prefix, required=True),
    repo_branch=read_vars('repo_branch', var_prefix=prefix, default='main'),
    dbt_env=parse_dict(read_vars('env', var_prefix=prefix, required=True)),
    notification_emails=read_vars('notification_emails')
)

for dag_id, dag in dag_map.items():
    globals()[dag_id] = dag
