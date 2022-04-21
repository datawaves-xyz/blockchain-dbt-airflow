import json
import logging
import subprocess
from typing import Optional, Dict

from airflow import AirflowException


class DbtCli:
    """
    Simple wrapper around the dbt CLI.

    :param profiles_dir: If set, passed as the `--profiles-dir` argument to the `dbt` command
    :param target: If set, passed as the `--target` argument to the `dbt` command
    :param dir: The directory to run the CLI in
    :param vars: If set, passed as the `--vars` argument to the `dbt` command
    :param full_refresh: If `True`, will fully-refresh incremental models.
    :param models: If set, passed as the `--models` argument to the `dbt` command
    :param warn_error: If `True`, treat warnings as errors.
    :param exclude: If set, passed as the `--exclude` argument to the `dbt` command
    :param select: If set, passed as the `--select` argument to the `dbt` command
    :param selector: If set, passed as the `--selector` argument to the `dbt` command
    :param dbt_bin: The `dbt` CLI. Defaults to `dbt`, so assumes it's on your `PATH`
    :param output_encoding: Output encoding of bash command. Defaults to utf-8
    :param verbose: The operator will log verbosely to the Airflow logs
    :type verbose: bool
    """

    def __init__(
            self,
            profiles_dir: Optional[str] = None,
            target: Optional[str] = None,
            dir: str = '.',
            vars: Optional[Dict[str, any]] = None,
            full_refresh: bool = False,
            data: bool = False,
            schema: bool = False,
            models: Optional[str] = None,
            exclude: Optional[str] = None,
            select: Optional[str] = None,
            selector: Optional[str] = None,
            dbt_bin: str = 'dbt',
            output_encoding: str = 'utf-8',
            verbose: bool = True,
            warn_error: bool = False
    ) -> None:
        self.profiles_dir = profiles_dir
        self.dir = dir
        self.target = target
        self.vars = vars
        self.full_refresh = full_refresh
        self.data = data
        self.schema = schema
        self.models = models
        self.exclude = exclude
        self.select = select
        self.selector = selector
        self.dbt_bin = dbt_bin
        self.verbose = verbose
        self.warn_error = warn_error
        self.output_encoding = output_encoding
        self.logger = logging.getLogger(self.__class__.__name__)

    def _dump_vars(self):
        # The dbt `vars` parameter is defined using YAML. Unfortunately the standard YAML library
        # for Python isn't very good and I couldn't find an easy way to have it formatted
        # correctly. However, as YAML is a super-set of JSON, this works just fine.
        return json.dumps(self.vars)

    def run_cli(self, *command):
        """
        Run the dbt cli
        :param command: The dbt command to run
        :type command: str
        """

        dbt_cmd = [self.dbt_bin, *command]

        if self.profiles_dir is not None:
            dbt_cmd.extend(['--profiles-dir', self.profiles_dir])

        if self.target is not None:
            dbt_cmd.extend(['--target', self.target])

        if self.vars is not None:
            dbt_cmd.extend(['--vars', self._dump_vars()])

        if self.data:
            dbt_cmd.extend(['--data'])

        if self.schema:
            dbt_cmd.extend(['--schema'])

        if self.models is not None:
            dbt_cmd.extend(['--models', self.models])

        if self.exclude is not None:
            dbt_cmd.extend(['--exclude', self.exclude])

        if self.select is not None:
            dbt_cmd.extend(['--select', self.select])

        if self.selector is not None:
            dbt_cmd.extend(['--selector', self.selector])

        if self.full_refresh:
            dbt_cmd.extend(['--full-refresh'])

        if self.warn_error:
            dbt_cmd.insert(1, '--warn-error')

        if self.verbose:
            self.logger.info(" ".join(dbt_cmd))

        sp = subprocess.Popen(
            dbt_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=self.dir,
            close_fds=True)

        self.logger.info("Output:")
        for line in iter(sp.stdout.readline, b''):
            line = line.decode(self.output_encoding).rstrip()
            self.logger.info(line)
        sp.wait()

        self.logger.info(
            "Command exited with return code %s",
            sp.returncode
        )

        if sp.returncode:
            raise AirflowException("dbt command failed")

    @staticmethod
    def deps(
            dir: str, profiles_dir: Optional[str] = None, target: Optional[str] = None, **kwargs
    ) -> None:
        DbtCli(dir=dir, profiles_dir=profiles_dir, target=target, **kwargs).run_cli('deps')
