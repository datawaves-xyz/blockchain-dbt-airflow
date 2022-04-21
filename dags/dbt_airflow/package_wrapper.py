import functools
import os.path
import pathlib
import shutil
from typing import List, Dict

from fal import FalDbt, DbtModel
from faldbt.project import DbtTest

from dbt_airflow.git_cli import GitCli
from dbt_airflow.profile import Profile


class PackageWrapper:
    _package: FalDbt

    def __init__(
            self,
            repo_url: str,
            repo_tag: str = 'master',
            workspace: str = '/tmp/dbt',
            profiles_dir: str = '/tmp/profiles'
    ) -> None:
        # Pull the code of the dbt project and get the local absolute path of the project.
        project_dir = os.path.join(workspace, self._get_project_name(repo_url))

        if os.path.exists(project_dir):
            shutil.rmtree(project_dir)

        GitCli.clone(repo_url=repo_url, dir=workspace, tag=repo_tag)

        # Generate the profiles.yml file
        # we need to write file because of there is not the constructor method with Profile parameter in the FalDbt.
        if os.path.exists(profiles_dir):
            shutil.rmtree(profiles_dir)

        pathlib.Path(profiles_dir).mkdir(parents=True)
        profile = Profile()
        profile.dump_yml_file(profile_dir=profiles_dir)

        self._package = FalDbt(project_dir=project_dir, profiles_dir=profiles_dir)

    @functools.cached_property
    def models(self) -> List[DbtModel]:
        return self._package.list_models()

    @functools.cached_property
    def tests(self) -> List[DbtTest]:
        return self._package.list_tests()

    @functools.cached_property
    def model_grouping_by_tag(self) -> Dict[str, List[DbtModel]]:
        grouping_by_tags: Dict[str, List[DbtModel]] = {}

        for model in self.models:
            merged_tag = self._merge_tags(model.node.tags)

            if merged_tag not in grouping_by_tags:
                grouping_by_tags[merged_tag] = []

            grouping_by_tags[merged_tag].append(model)

        return grouping_by_tags

    @staticmethod
    def get_level(tag: str) -> str:
        return tag.split('_')[1]

    @staticmethod
    def _merge_tags(tags: List[str]) -> str:
        chain = None
        proj = None
        level = None

        for tag in tags:
            if tag.startswith('chain_'):
                chain = tag[len('chain_'):]
            elif tag.startswith('proj_'):
                proj = tag[len('proj_')]
            elif tag.startswith('level_'):
                level = tag[len('level_')]

        if chain is None or proj is None or level is None:
            raise ValueError(f'{",".join(tags)} is not a valid tags.')

        return f'{chain}_{level}_{proj}'

    @staticmethod
    def _get_project_name(repo_url: str) -> str:
        return repo_url.split('/')[-1][:-4]
