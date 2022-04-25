import functools
import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Union, Optional

import requests
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedModelNode, ParsedSourceDefinition
from dbt.contracts.results import NodeStatus
from dbt.node_types import NodeType


def normalize_path(base: str, path: Union[Path, str]):
    return Path(os.path.realpath(os.path.join(base, path)))


def normalize_paths(base: str, paths: Union[List[Path], List[str]]) -> List[Path]:
    return list(map(lambda path: normalize_path(base, path), paths))


@dataclass
class DbtTest:
    node: Any
    name: str = field(init=False)
    model: str = field(init=False)
    column: str = field(init=False)
    status: str = field(init=False)

    def __post_init__(self):
        node = self.node
        self.unique_id = node.unique_id
        self.logger = logging.Logger(self.__class__.__name__)
        if node.resource_type == NodeType.Test:
            if hasattr(node, "test_metadata"):
                self.name = node.test_metadata.name

                # node.test_metadata.kwargs looks like this:
                # kwargs={'column_name': 'y', 'model': "{{ get_where_subquery(ref('boston')) }}"}
                # and we want to get 'boston' is the model name that we want extract
                # except in dbt v 0.20.1, where we need to filter 'where' string out
                refs = re.findall(r"'([^']+)'", node.test_metadata.kwargs["model"])
                self.model = [ref for ref in refs if ref != "where"][0]
                self.column = node.test_metadata.kwargs.get("column_name", None)
            else:
                self.logger.debug(f"Non-generic test was not processed: {node.name}")

    def set_status(self, status: str):
        self.status = status


@dataclass
class DbtModel:
    node: ParsedModelNode
    name: str = field(init=False)
    meta: Dict[str, Any] = field(init=False)
    status: NodeStatus = field(init=False)
    columns: Dict[str, Any] = field(init=False)
    tests: List[DbtTest] = field(init=False)

    def __post_init__(self):
        node = self.node
        self.name = node.name

        # BACKWARDS: Change intorduced in XXX (0.20?)
        # TODO: specify which version is for this
        if hasattr(node.config, "meta"):
            self.meta = node.config.meta
        elif hasattr(node, "meta"):
            self.meta = node.meta

        self.columns = node.columns
        self.unique_id = node.unique_id
        self.tests = []

    def __hash__(self) -> int:
        return self.unique_id.__hash__()

    def get_depends_on_nodes(self) -> List[str]:
        return self.node.depends_on_nodes

    def get_script_paths(
            self, keyword: str, scripts_dir: str, before: bool
    ) -> List[Path]:
        return normalize_paths(scripts_dir, self.get_scripts(keyword, before))

    def get_scripts(self, keyword: str, before: bool) -> List[str]:
        # sometimes `scripts` can *be* there and still be None
        if self.meta.get(keyword):
            scripts_node = self.meta[keyword].get("scripts")
            if not scripts_node:
                return []
            if isinstance(scripts_node, list) and before:
                return []
            if before:
                return scripts_node.get("before") or []
            if isinstance(scripts_node, list):
                return scripts_node
            return scripts_node.get("after") or []
        else:
            return []

    def set_status(self, status: str):
        self.status = NodeStatus(status)


@dataclass
class DbtManifest:
    nativeManifest: Manifest

    @functools.cached_property
    def models(self) -> List[DbtModel]:
        return list(
            filter(
                lambda model: model.node.resource_type == NodeType.Model,
                map(
                    lambda node: DbtModel(node=node), self.nativeManifest.nodes.values()
                ),
            )
        )

    @functools.cached_property
    def tests(self) -> List[DbtTest]:
        return list(
            filter(
                lambda test: test.node.resource_type == NodeType.Test,
                map(
                    lambda node: DbtTest(node=node), self.nativeManifest.nodes.values()
                ),
            )
        )

    @functools.cached_property
    def sources(self) -> List[ParsedSourceDefinition]:
        return list(self.nativeManifest.sources.values())

    @functools.cached_property
    def model_grouping_by_tag(self) -> Dict[str, List[DbtModel]]:
        grouping_by_tags: Dict[str, List[DbtModel]] = {}

        for model in self.models:
            merged_tag = self._merge_tags(model.node.tags)

            if merged_tag is None:
                continue

            if merged_tag not in grouping_by_tags:
                grouping_by_tags[merged_tag] = []

            grouping_by_tags[merged_tag].append(model)

        return grouping_by_tags

    @classmethod
    def from_url(cls, url: str) -> 'DbtManifest':
        res = requests.get(url)
        return cls(Manifest.from_dict(res.json()))

    @classmethod
    def from_file(cls, file: str) -> 'DbtManifest':
        with open(file, 'r') as f:
            return cls(Manifest.from_dict(json.loads(f.read())))

    @staticmethod
    def _merge_tags(tags: List[str]) -> Optional[str]:
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
            return None

        return f'{chain}_{level}_{proj}'
