import logging

from dbt_airflow.build_dbt_dag import DbtDagsBuilder
from variables import read_vars

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

builder = DbtDagsBuilder(
    start_date='2022-04-20',
    notification_emails=read_vars('notification_emails')
)

for dag_id, dag in builder.build_dbt_dags().items():
    globals()[dag_id] = dag
