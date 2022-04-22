import logging

from utils import exec_command


class DbtProject:

    def __init__(self, project_path: str):
        self.project_path = project_path
        self.logger = logging.getLogger(self.__class__.__name__)

        exec_command(
            cmd=['dbt', '--profiles-dir', '.', 'deps'],
            cwd=self.project_path, logger=self.logger
        )

    # def __init__(
    #         self, workspace: str, repo_url: str, repo_branch: str
    # ) -> None:
    #     self.logger = logging.Logger(self.__class__.__name__)
    #     self.workspace = workspace
    #     self.project_name = self.get_name_from_git_url(repo_url)
    #     self.project_path = os.path.join(self.workspace, self.project_name)
    #
    #     if os.path.exists(self.project_path):
    #         shutil.rmtree(self.project_path)
    #
    #     exec_command(
    #         cmd=['git', 'clone', '--branch', repo_branch, repo_url],
    #         cwd=self.workspace, logger=self.logger
    #     )
    #
    #     exec_command(
    #         cmd=['dbt', '--profiles-dir', '.', 'deps'],
    #         cwd=self.project_path, logger=self.logger
    #     )
    #
    # @staticmethod
    # def get_name_from_git_url(url: str) -> str:
    #     return url.split('/')[-1][:-4]
