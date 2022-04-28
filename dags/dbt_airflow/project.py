import logging
import os.path
import pathlib
from logging import Logger
from typing import Optional, List

from dbt_airflow.dbt_resource import DbtManifest
from utils import exec_command

package_path = '/tmp/dbt-package'
target_path = '/tmp/dbt-target'


class DbtProject:

    def __init__(
            self, project_path: str, logger: Logger
    ) -> None:
        self.project_path = project_path

        if not os.path.exists(package_path):
            exec_command(
                cmd=['dbt', '--profiles-dir', 'profile', 'deps'],
                cwd=self.project_path, logger=logger
            )

        # if os.path.exists(package_path):
        #     shutil.rmtree(package_path)

        # if os.path.exists(target_path):
        #     shutil.rmtree(target_path)

        # shutil.copytree(self.target_path, target_path)
        # shutil.copytree(self.package_path, package_path)

    @property
    def manifest(self) -> DbtManifest:
        return DbtManifest.from_file(os.path.join(self.project_path, 'manifest.json'))

    @property
    def target_path(self) -> str:
        return os.path.join(self.project_path, 'target')

    @property
    def package_path(self) -> str:
        return os.path.join(self.project_path, 'dbt_packages')


class DbtWorkspace:

    def __init__(
            self, root_folder: Optional[str] = None
    ) -> None:
        self.projects: List[DbtProject] = []
        self.logger = logging.getLogger(self.__class__.__name__)

        if root_folder is None:
            folder = os.path.dirname(__file__)
            root_folder = pathlib.Path(folder).parent.parent

        for sub_folder in os.listdir(root_folder):
            absolute_path = os.path.join(root_folder, sub_folder)
            if os.path.isdir(absolute_path) and sub_folder.startswith('dbt_'):
                self.projects.append(DbtProject(project_path=absolute_path, logger=self.logger))
