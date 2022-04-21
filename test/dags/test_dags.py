# import json
# import unittest
# from typing import List, AnyStr
# from unittest import mock
#
# from dbt.contracts.graph.manifest import Manifest
# from faldbt.project import DbtModel, DbtManifest
#
# import test
#
# RESOURCE_GROUP = 'dags'
#
#
# def _get_resource_path(file_name: str) -> AnyStr:
#     return test.get_resource_path([RESOURCE_GROUP], file_name)
#
#
# def _read_resource(file_name: str) -> AnyStr:
#     return test.read_resource([RESOURCE_GROUP], file_name)
#
#
# def patch_package_wrapper_init(
#         self, repo_url: str, repo_tag: str, workspace: str, profiles_dir: str
# ) -> None:
#     """
#     这个函数本来有两个动作：
#         1. 通过 git 拉取一个dbt仓库
#         2. 创建一个 FalDbt 实例
#     为了让测试不依赖外部提供 patch 版本，屏蔽掉这些动作，
#     并且配合 patch model_grouping_by_tag 方法来实现 Mock manifest 文件的效果
#     """
#     pass
#
#
# def patch_package_wrapper_models(self) -> List[DbtModel]:
#     manifest = DbtManifest(Manifest.from_dict(json.loads(_read_resource('manifest.json'))))
#     return manifest.get_models()
#
#
# class AirflowDagTest(unittest.TestCase):
#
#     def test_dbt_dags(self):
#         with mock.patch(
#                 'dbt_airflow.package_wrapper.__init__', patch_package_wrapper_init
#         ), mock.patch(
#             'dbt_airflow.package_wrapper.models', patch_package_wrapper_models
#         ):
