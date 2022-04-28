import subprocess
from datetime import datetime, timedelta
from logging import Logger
from typing import List

import pendulum as pdl
from airflow import AirflowException
from airflow.models import DAG, BaseOperator
from airflow.sensors.external_task import ExternalTaskSensor
from croniter import croniter

one_hour_seconds = timedelta(hours=1).total_seconds()
one_day_seconds = timedelta(days=1).total_seconds()
one_week_seconds = timedelta(weeks=1).total_seconds()


def get_window_unit(dag: DAG) -> str:
    cron = croniter(dag.schedule_interval)
    prev_dt: datetime = cron.get_prev(datetime)
    next_dt: datetime = cron.get_next(datetime)
    delta_second = abs(int(prev_dt.timestamp()) - int(next_dt.timestamp()))

    if delta_second == one_hour_seconds:
        return 'hour'
    elif delta_second == one_day_seconds:
        return 'day'
    elif delta_second == one_week_seconds:
        return 'week'
    else:
        return 'others'


def new_same_window_external_sensor(
        dag: DAG, depend_task: BaseOperator
) -> ExternalTaskSensor:
    depend_dag = depend_task.dag

    def execute_date_fn(execution_date: datetime) -> datetime:
        window_unit = get_window_unit(dag)
        d_window_unit = get_window_unit(depend_dag)

        if window_unit == 'others' or d_window_unit == 'others' or window_unit != d_window_unit:
            raise ValueError(
                'The dag({},{}) and the upstream dag({},{}) must have same window type, and can not be others.',
                dag.schedule_interval, window_unit, depend_dag.schedule_interval, d_window_unit)

        ex_date = pdl.instance(execution_date)
        window_start_of = ex_date.start_of(window_unit)
        window_end_of = ex_date.end_of(window_unit)
        upstream_spots = depend_dag.date_range(start_date=window_start_of, end_date=window_end_of)
        if len(upstream_spots) == 0:
            raise AirflowException("The upstream task can not be found in {} to {}.", window_start_of,
                                   window_end_of)
        # AS the default first spot is window_start_of
        # use the last one in case of window_start_of is not a valid execution date.
        return upstream_spots[-1]

    return ExternalTaskSensor(
        dag=dag,
        task_id=f'wait_{depend_task.dag_id}_{depend_task.task_id}',
        external_dag_id=depend_task.dag_id,
        external_task_id=depend_task.task_id,
        check_existence=True,
        execution_date_fn=execute_date_fn,
        mode='reschedule',
    )


def exec_command(
        cmd: List[str], cwd: str, logger: Logger
) -> None:
    sp = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=cwd,
        close_fds=True
    )

    logger.info(f"exec command {cmd} in the {cwd}")
    logger.info("output:")
    for line in iter(sp.stdout.readline, b''):
        line = line.decode('utf-8').rstrip()
        logger.info(line)
    sp.wait()

    logger.info(
        "command exited with return code %s",
        sp.returncode
    )

    if sp.returncode:
        err_msg = ','.join([line.decode('utf-8').rstrip()
                            for line in iter(sp.stderr.readline, b'')])
        raise AirflowException(f"exec command failed, cmd: {cmd}, err_msg: {err_msg}")
