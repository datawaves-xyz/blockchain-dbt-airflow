import contextlib
import os
import signal
from subprocess import PIPE, STDOUT, Popen
from tempfile import TemporaryDirectory, gettempdir
from typing import Dict, List, Optional

from airflow.hooks.subprocess import SubprocessHook, SubprocessResult


class FixedSubprocessHook(SubprocessHook):
    def run_command(
            self,
            command: List[str],
            env: Optional[Dict[str, str]] = None,
            output_encoding: str = 'utf-8',
            cwd: str = None,
    ) -> SubprocessResult:
        """
        Execute the command.

        If ``cwd`` is None, execute the command in a temporary directory which will be cleaned afterwards.
        If ``env`` is not supplied, ``os.environ`` is passed

        :param command: the command to run
        :param env: Optional dict containing environment variables to be made available to the shell
            environment in which ``command`` will be executed.  If omitted, ``os.environ`` will be used.
            Note, that in case you have Sentry configured, original variables from the environment
            will also be passed to the subprocess with ``SUBPROCESS_`` prefix. See
            :doc:`/logging-monitoring/errors` for details.
        :param output_encoding: encoding to use for decoding stdout
        :param cwd: Working directory to run the command in.
            If None (default), the command is run in a temporary directory.
        :return: :class:`namedtuple` containing ``exit_code`` and ``output``, the last line from stderr
            or stdout
        """
        self.log.info('Tmp dir root location: \n %s', gettempdir())
        with contextlib.ExitStack() as stack:
            if cwd is None:
                cwd = stack.enter_context(TemporaryDirectory(prefix='airflowtmp'))

            def pre_exec():
                # Restore default signal disposition and invoke setsid
                for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                    if hasattr(signal, sig):
                        signal.signal(getattr(signal, sig), signal.SIG_DFL)
                os.setsid()

            self.log.info('Running command: %s', command)

            self.sub_process = Popen(
                command,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=cwd,
                env=env if env or env == {} else os.environ,
                preexec_fn=pre_exec,
            )

            self.log.info('Output:')
            line = ''
            lines = []
            for raw_line in iter(self.sub_process.stdout.readline, b''):
                line = raw_line.decode(output_encoding).rstrip()
                lines.append(line)
                self.log.info("%s", line)

            self.sub_process.wait()

            self.log.info('Command exited with return code %s', self.sub_process.returncode)

        return SubprocessResult(exit_code=self.sub_process.returncode, output='|'.join(lines))
