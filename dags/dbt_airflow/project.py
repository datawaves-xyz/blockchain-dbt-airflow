import logging
import os.path

from utils import exec_command


class DbtProject:

    def __init__(
            self, workspace: str, repo_url: str, repo_branch: str
    ) -> None:
        self.logger = logging.Logger(self.__class__.__name__)
        self.workspace = workspace
        self.project_name = self.get_name_from_git_url(repo_url)
        self.project_path = os.path.join(self.workspace, self.project_name)

        exec_command(
            cmd=f'git clone --branch {repo_branch} {repo_url}',
            cwd=workspace, logger=self.logger
        )

    @staticmethod
    def get_name_from_git_url(url: str) -> str:
        return url.split('/')[-1][:-4]
