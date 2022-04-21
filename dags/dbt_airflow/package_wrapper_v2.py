import functools
import json
import os
import pathlib
from typing import List, Dict

from dbt.contracts.graph.manifest import Manifest
from fal import DbtModel
from faldbt.project import DbtManifest, DbtTest


class PackageWrapperV2:
    manifest: DbtManifest

    def __init__(self):
        with open(self.get_manifest_path(), 'r') as f:
            self.manifest = DbtManifest(Manifest.from_dict(json.loads(f.read())))

    @functools.cached_property
    def models(self) -> List[DbtModel]:
        return self.manifest.get_models()

    @functools.cached_property
    def tests(self) -> List[DbtTest]:
        return self.manifest.get_tests()

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
                proj = tag[len('proj_'):]
            elif tag.startswith('level_'):
                level = tag[len('level_'):]

        if chain is None or proj is None or level is None:
            raise ValueError(f'{",".join(tags)} is not a valid tags.')

        return f'{chain}_{level}_{proj}'

    @staticmethod
    def get_manifest_path() -> str:
        dbt_root_dir = os.path.dirname(__file__)
        dag_root_dir = pathlib.Path(dbt_root_dir).parent.absolute()
        return os.path.join(dag_root_dir, 'manifest.json')
