import os.path
import pathlib
from datetime import timedelta
from typing import Optional, Dict, TypeVar

from airflow import models as m, DAG
from airflow.operators.bash import BashOperator
from faldbt.project import DbtModel

from dags.dbt_airflow.package_wrapper import PackageWrapper
from dags.utils import new_same_window_external_sensor

V = TypeVar('V', bound=BashOperator)


class DbtDagsBuilder:
    """
     在所有dbt models上会使用tags来设置models的属性：
      - chain_%s: 表示model所属的链，如：chain_ethereum
      - proj_%s: 表示model所属的项目，可以是抽象的如：proj_opensea，也可以是具象的：proj_nft
      - level_%s: 表示model所属的层，如：level_parse
        - standardize: 标准化层，对所有的source进行标准化处理，之后作为之后所有表的依赖
        - parse: 解析层，对 traces/logs 表进行解析，形成具体的业务表
        - modeling：建模层，使用业务表构建上层逻辑

     这三者的任意组合会对应一个DAG。由于层级之间的依赖是单向的，所以调度时间会绑定 level tag，跨层级的依赖会通过构建 sensor 实现。
    """

    _task_map: Dict[str, V]
    _default_dag_args: Dict[str, any]
    _schedule_interval_map: Dict[str, str]

    def __init__(
            self,
            start_date: str,
            standardize_schedule_interval: str = '0 1 * * *',
            parse_schedule_interval: str = '0 2 * * *',
            modeling_schedule_interval: str = '0 3 * * *',
            notification_emails: Optional[str] = None
    ) -> None:
        self._task_map = {}

        self._schedule_interval_map = {
            'standardize': standardize_schedule_interval,
            'parse': parse_schedule_interval,
            'modeling': modeling_schedule_interval
        }

        self._default_dag_args = {
            'depends_on_past': False,
            'start_date': start_date,
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 3,
            'retry_delay': timedelta(minutes=1)
        }

        if notification_emails and len(notification_emails) > 0:
            self._default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    def build_dbt_dags(
            self, repo_url: str, workspace: str, profiles_dir: str,
    ) -> None:
        if not os.path.exists(workspace):
            pathlib.Path(workspace).mkdir(parents=True)

        if not os.path.exists(profiles_dir):
            pathlib.Path(profiles_dir).mkdir(parents=True)

        package = PackageWrapper(repo_url=repo_url, workspace=workspace, profiles_dir=profiles_dir)
        model_maps = package.model_grouping_by_tag

        # Build all DAGs and all tasks in every DAG
        for tag, models in model_maps.items():
            level = PackageWrapper.get_level(tag)

            dag = m.DAG(
                dag_id=tag,
                catchup=False,
                schedule_interval=self._schedule_interval_map[level],
                default_args=self._default_dag_args
            )

            for model in models:
                self._make_dbt_run_task(model=model, dag=dag)

        # Build all dependency relationship
        for models in model_maps.values():
            for model in models:
                unique_id = model.node.unique_id
                depends = model.node.depends_on_nodes
                task = self._task_map[unique_id]

                for depend in depends:
                    depend_type = depend.split('.')[0]

                    # TODO: 支持对 source depend 的解析，和 extract enrich 任务挂钩
                    if depend_type != 'models':
                        continue

                    depend_task = self._task_map[depend]
                    if depend_task.dag_id == task.dag_id:
                        depend_task >> task
                    else:
                        # sensor_id 需要加上 task.dag_id 前缀是因为可能多个 DAG 中都需要加入 wait 同一个任务的sensor，
                        # 这时候它们其实是所属不同 DAG 的不同 sensor 实例
                        sensor_id = f'{task.dag_id}_{depend_task.dag_id}_{depend_task.task_id}'
                        sensor_task = self._task_map[sensor_id]
                        if sensor_task is None:
                            sensor_task = new_same_window_external_sensor(dag=task.dag, depend_task=depend_task)
                            self._task_map[sensor_id] = sensor_task
                        sensor_task >> task

    def _make_dbt_run_task(
            self, model: DbtModel, dag: DAG
    ) -> BashOperator:
        dbt_dir = model.node.root_path
        unique_id = model.node.unique_id
        model_path = model.node.path
        model_name = model.name.split('.')[-1]

        operator = BashOperator(
            task_id=model_name,
            bash_command=f'dbt --no-write-json run --select {model_path}',
            cwd=dbt_dir,
            dag=dag
        )

        self._task_map[unique_id] = operator
        return operator
