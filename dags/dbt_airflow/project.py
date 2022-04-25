import os.path
import pathlib
from typing import Optional

from dbt_airflow.dbt_resource import DbtManifest


class DbtProject:

    def __init__(self, project_path: str):
        self.project_path = project_path

    @property
    def manifest(self) -> DbtManifest:
        return DbtManifest.from_file(os.path.join(self.project_path, 'target', 'manifest.json'))


class DbtWorkspace:

    def __init__(self, root_folder: Optional[str] = None):
        self.projects = []

        if root_folder is None:
            folder = os.path.dirname(__file__)
            root_folder = pathlib.Path(folder).parent.parent

        for sub_folder in os.listdir(root_folder):
            absolute_path = os.path.join(root_folder, sub_folder)
            if os.path.isdir(absolute_path) and sub_folder.startswith('dbt_'):
                self.projects.append(DbtProject(project_path=absolute_path))
