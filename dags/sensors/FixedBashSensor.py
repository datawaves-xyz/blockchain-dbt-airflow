import os
from functools import cached_property
from typing import Optional, Dict

from airflow import AirflowException
from airflow.hooks.subprocess import SubprocessHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.operator_helpers import context_to_airflow_vars


class FixedBashSensor(BaseSensorOperator):
    """
    Executes a bash command/script and returns True if and only if the
    return code is 0.

    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed.
    :type bash_command: str

    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :type env: dict
    :param output_encoding: output encoding of bash command.
    :type output_encoding: str
    :param cwd: Working directory to execute the command in.
        If None (default), the command is run in a temporary directory.
    :type cwd: str
    """

    template_fields = ('bash_command', 'env')

    def __init__(
            self,
            *,
            bash_command: str,
            env: Optional[Dict[str, str]] = None,
            output_encoding: str = 'utf-8',
            cwd: Optional[str] = None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.bash_command = bash_command
        self.env = env
        self.output_encoding = output_encoding
        self.cwd = cwd

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command"""
        return SubprocessHook()

    def get_env(self, context):
        """Builds the set of environment variables to be exposed for the bash command"""
        env = self.env
        if env is None:
            env = os.environ.copy()

        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.debug(
            'Exporting the following env vars:\n%s',
            '\n'.join(f"{k}={v}" for k, v in airflow_context_vars.items()),
        )
        env.update(airflow_context_vars)
        return env

    def poke(self, context):
        if self.cwd is not None:
            if not os.path.exists(self.cwd):
                raise AirflowException(f"Can not find the cwd: {self.cwd}")
            if not os.path.isdir(self.cwd):
                raise AirflowException(f"The cwd {self.cwd} must be a directory")

        env = self.get_env(context)
        result = self.subprocess_hook.run_command(
            command=['bash', '-c', self.bash_command],
            env=env,
            output_encoding=self.output_encoding,
            cwd=self.cwd,
        )

        return not result.exit_code
