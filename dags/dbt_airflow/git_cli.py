import logging
import subprocess
from typing import Optional, List

from airflow import AirflowException


class GitCli:
    """
    Simple wrapper around the git CLI.

    :param repo_url: Required, the url of the repository
    :param dir: The directory to run the CLI in
    :param one_revision: If `True`, skip downloading all the history up to that revision
    :param tag: If set, passed as the `--branch tag` argument to the `git` command
    :param git_bin: The `git` CLI. Defaults to `git`, so assumes it's on your `PATH`
    :param output_encoding: Output encoding of bash command. Defaults to utf-8
    :param verbose: The operator will log verbosely to the Airflow logs
    """

    def __init__(
            self,
            repo_url: str,
            dir='..',
            one_revision=True,
            tag: Optional[str] = None,
            git_bin='git',
            output_encoding='utf-8',
            verbose=True
    ) -> None:
        self.repo_url = repo_url
        self.dir = dir
        self.one_revision = one_revision
        self.tag = tag
        self.git_bin = git_bin
        self.output_encoding = output_encoding
        self.verbose = verbose
        self.logger = logging.getLogger(self.__class__.__name__)

    def run_cli(self, *command):
        git_cmd: List[str] = [self.git_bin, *command]

        if self.one_revision:
            git_cmd.extend(['--depth', '1'])

        if self.tag:
            git_cmd.extend(['--branch', self.tag])

        git_cmd.extend([self.repo_url])

        if self.verbose:
            self.logger.info(" ".join(git_cmd))

        sp = subprocess.Popen(
            git_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=self.dir,
            close_fds=True
        )

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
            raise AirflowException("git command failed")

    @staticmethod
    def clone(
            repo_url: str, dir: str, tag: str, **kwargs
    ) -> None:
        GitCli(repo_url=repo_url, dir=dir, tag=tag, **kwargs).run_cli('clone')
