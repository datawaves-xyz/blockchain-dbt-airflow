import json
import os
import pathlib
from datetime import timedelta
from typing import Optional, TypeVar, Dict

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator

from dbt_airflow.dbt_resource import DbtModel
from dbt_airflow.project import DbtWorkspace
from utils import new_same_window_external_sensor

Operator = TypeVar('Operator', bound=BaseOperator)


def _get_dbt_project_path() -> str:
    folder = os.path.dirname(__file__)
    root_folder = pathlib.Path(folder).parent.parent
    return os.path.join(root_folder, 'dbt_project')


def _get_level(tag: str) -> str:
    return tag.split('_')[1]


def _make_dbt_run_task(
        project: str,
        model: DbtModel,
        dag: DAG,
        variables: Dict[str, any],
        env: Optional[Dict[str, any]] = None,
) -> BashOperator:
    model_full_name = '.'.join(model.node.fqn)
    model_name = model.name.split('.')[-1]

    operator = BashOperator(
        task_id=model_name,
        bash_command=f"/home/airflow/.local/bin/dbt --profiles-dir . run --vars '{json.dumps(variables)}' --select {model_full_name}",
        cwd=project, env=env, dag=dag
    )

    return operator


def build_dbt_dags(
        start_date: str,
        dbt_env: Optional[Dict[str, any]] = None,
        standardize_schedule_interval: str = '0 1 * * *',
        parse_schedule_interval: str = '0 2 * * *',
        modeling_schedule_interval: str = '0 3 * * *',
        notification_emails: Optional[str] = None
) -> Dict[str, DAG]:
    schedule_interval_map = {
        'standardize': standardize_schedule_interval,
        'parse': parse_schedule_interval,
        'modeling': modeling_schedule_interval
    }

    default_dag_args = {
        'depends_on_past': False,
        'start_date': start_date,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=1)
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    task_map: Dict[str, Operator] = {}
    dag_map: Dict[str, DAG] = {}
    workspace = DbtWorkspace()

    for project in workspace.projects:

        # Build all DAGs and all tasks in every DAG
        for tag, models in project.manifest.model_grouping_by_tag.items():
            level = _get_level(tag)

            dag = DAG(
                dag_id=tag,
                catchup=False,
                schedule_interval=schedule_interval_map[level],
                default_args=default_dag_args
            )

            dag_map[tag] = dag

            for model in models:
                task = _make_dbt_run_task(project=project.project_path, model=model, dag=dag, env=dbt_env,
                                          variables={'dt': '{{ ds }}'})
                task_map[model.node.unique_id] = task

        # Build all dependency relationship
        for models in project.manifest.model_grouping_by_tag.values():
            for model in models:
                unique_id = model.node.unique_id
                depends = model.node.depends_on_nodes
                task = task_map[unique_id]

                for depend in depends:
                    depend_type = depend.split('.')[0]

                    # TODO: 支持对 source depend 的解析，和 extract enrich 任务挂钩
                    if depend_type != 'model':
                        continue

                    depend_task = task_map[depend]
                    if depend_task.dag_id == task.dag_id:
                        depend_task >> task
                    else:
                        # sensor_id 需要加上 task.dag_id 前缀是因为可能多个 DAG 中都需要加入 wait 同一个任务的sensor，
                        # 这时候它们其实是所属不同 DAG 的不同 sensor 实例
                        sensor_id = f'{task.dag_id}.{depend_task.dag_id}.{depend_task.task_id}'
                        if sensor_id not in task_map:
                            sensor_task = new_same_window_external_sensor(dag=task.dag, depend_task=depend_task)
                            task_map[sensor_id] = sensor_task
                        else:
                            sensor_task = task_map[sensor_id]
                        sensor_task >> task

    return dag_map
