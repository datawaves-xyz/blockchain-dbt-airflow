import logging
import subprocess
from typing import Optional

from airflow import AirflowException
from pydantic import BaseModel


class GitCli(BaseModel):
    """
    Simple wrapper around the git CLI.
    """

    # The directory to run the CLI in
    dir = '..'
    # If `True`, skip downloading all the history up to that revision
    one_revision = True
    # If set, passed as the `--branch tag` argument to the `git` command
    tag: Optional[str] = None
    # Required, the url of the repository
    repo_url: str
    # The `git` CLI. Defaults to `git`, so assumes it's on your `PATH`
    git_bin = 'git'
    # Output encoding of bash command. Defaults to utf-8
    output_encoding = 'uft-8'
    # The operator will log verbosely to the Airflow logs
    verbose = True

    logger = logging.getLogger("GitCli")

    def run_cli(self, *command):
        git_cmd = [self.git_bin, *command]

        if self.one_revision:
            git_cmd.extend(['--depth', 1])

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
            raise AirflowException("git command failed")

    @staticmethod
    def clone(repo_url: str, **kwargs):
        GitCli(repo_url=repo_url, **kwargs).run_cli('clone')
