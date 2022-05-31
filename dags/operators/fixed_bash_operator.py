import os
from functools import cached_property
from typing import Callable, List, Optional

from airflow import AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator

from hooks.fixed_subprocess import FixedSubprocessHook


class FixedBashOperator(BashOperator):
    def __init__(self, failed_hook: Optional[Callable[[List[str]], None]] = None, **kwargs):
        super().__init__(**kwargs)
        self.failed_hook = failed_hook

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command"""
        return FixedSubprocessHook()

    def execute(self, context):
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
        if self.skip_exit_code is not None and result.exit_code == self.skip_exit_code:
            raise AirflowSkipException(f"Bash command returned exit code {self.skip_exit_code}. Skipping.")
        elif result.exit_code != 0:
            if self.failed_hook is not None:
                self.failed_hook(result.output)

            raise AirflowException(
                f'Bash command failed. The command returned a non-zero exit code {result.exit_code}.'
            )
        return result.output
