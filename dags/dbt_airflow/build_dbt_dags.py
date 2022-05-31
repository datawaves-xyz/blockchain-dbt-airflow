import json
import logging
import os
import pathlib
import re
from datetime import timedelta
from typing import Optional, TypeVar, Dict

import boto3
from airflow import DAG
from airflow.models import BaseOperator
from dbt.contracts.graph.parsed import ParsedSourceDefinition

from dbt_airflow.dbt_resource import DbtModel
from dbt_airflow.project import DbtWorkspace
from operators.fixed_bash_operator import FixedBashOperator
from sensors.fixed_bash_sensor import FixedBashSensor
from utils import new_same_window_external_sensor

Operator = TypeVar('Operator', bound=BaseOperator)


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
                task = _make_dbt_run_task(
                    project=project.project_path, model=model, dag=dag, env=dbt_env, variables={'dt': '{{ ds }}'})
                task_map[model.node.unique_id] = task

        # Build all dependency relationship
        for models in project.manifest.model_grouping_by_tag.values():
            for model in models:
                unique_id = model.node.unique_id
                depends = model.node.depends_on_nodes
                task = task_map[unique_id]

                for depend in depends:
                    depend_type = depend.split('.')[0]

                    if depend_type == 'source':
                        source = project.manifest.source_map[depend]
                        if not source.freshness:
                            continue

                        if source.unique_id not in task_map:
                            source_sensor = _make_dbt_freshness_sensor(
                                project=project.project_path, source=source, dag=task.dag, env=dbt_env)
                            task_map[source.unique_id] = source_sensor
                        else:
                            source_sensor = task_map[source.unique_id]
                        source_sensor >> task

                    elif depend_type == 'model':
                        depend_task = task_map[depend]
                        if depend_task.dag_id == task.dag_id:
                            depend_task >> task
                        else:
                            # sensor_id 需要加上 task.dag_id 前缀是因为可能多个 DAG 中都需要加入 wait 同一个任务的sensor，
                            # 这时候它们其实是所属不同 DAG 的不同 sensor 实例
                            sensor_id = f'sensor_{task.dag_id}.{depend_task.dag_id}.{depend_task.task_id}'
                            if sensor_id not in task_map:
                                sensor_task = new_same_window_external_sensor(dag=task.dag, depend_task=depend_task)
                                task_map[sensor_id] = sensor_task
                            else:
                                sensor_task = task_map[sensor_id]
                            sensor_task >> task

                    # TODO: support test

    return dag_map


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
) -> FixedBashOperator:
    def failed_hook(results: str) -> None:
        """When you attempt to rerun an Apache Spark write operation by cancelling the currently running job,
        the following error occurs:

        The associated location ('s3a://xxx') already exists.

        you can set [spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation=true] to resolve it when the spark
        version is 2.4 and below, but now can't do this. When a task fails check if it is because of it and if so simply
        delete the relevant folder.

        ref:
        - https://kb.databricks.com/jobs/spark-overwrite-cancel.html
        - https://stackoverflow.com/questions/55380427/azure-databricks-can-not-create-the-managed-table-the-associated-location-alre
        """
        regex = r"The associated location\('s3a://(.*?)'\) already exists."
        exception_str = 'at org.apache.spark.sql.errors.QueryCompilationErrors$.cannotOperateManagedTableWithExistingLocationError'
        s3_path = None

        if exception_str in results:
            matches = re.finditer(regex, results, re.MULTILINE)
            for _, match in enumerate(matches, start=1):
                s3_path = match.group(1)
                break

        if s3_path is None:
            logging.info("can't find any s3 path, do nothing in failed hook.")

        logging.info(f"s3_path: {s3_path}")
        words = s3_path.split('/')
        bucket = words[0]
        folder_path = '/'.join(words[1:])
        logging.info(f"delete s3 file, bucket: {bucket}, path_prefix: {folder_path}")

        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket)
        bucket.objects.filter(Prefix=f'{folder_path}/').delete()

    model_full_name = '.'.join(model.node.fqn)
    model_name = model.name.split('.')[-1]

    operator = FixedBashOperator(
        failed_hook=failed_hook,
        task_id=model_name,
        # https://stackoverflow.com/questions/63053009/how-can-we-check-the-output-of-bashoperator-in-airflow
        bash_command=f"/home/airflow/.local/bin/dbt --debug --cache-selected-only --profiles-dir profile run --vars '{json.dumps(variables)}' --select {model_full_name}",
        cwd=project, env=env, dag=dag,
    )

    return operator


def _make_dbt_freshness_sensor(
        project: str,
        source: ParsedSourceDefinition,
        dag: DAG,
        env: Optional[Dict[str, any]] = None
) -> FixedBashSensor:
    source_full_name = '.'.join(source.fqn)
    source_name = source.name

    sensor = FixedBashSensor(
        task_id=f'freshness_check_{source_name}',
        bash_command=f"/home/airflow/.local/bin/dbt --debug --cache-selected-only --profiles-dir profile source freshness --select source:{source_full_name}",
        cwd=project, env=env, dag=dag
    )

    return sensor
