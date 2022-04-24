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
