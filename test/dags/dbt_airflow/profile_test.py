import os.path
import tempfile
import unittest
from typing import AnyStr
from unittest.mock import patch

import yaml

import test
from dbt_airflow.profile import Profile

RESOURCE_GROUP = 'profiles'


def _get_resource_path(file_name: str) -> AnyStr:
    return test.get_resource_path([RESOURCE_GROUP], file_name)


def _read_resource(file_name: str) -> AnyStr:
    return test.read_resource([RESOURCE_GROUP], file_name)


class ProfileTest(unittest.TestCase):

    @patch.dict('os.environ', {
        'AIRFLOW_VAR_DBT_PROFILE_NAME': 'ethereum_source',
        'AIRFLOW_VAR_DBT_TARGET_NAME': 'dev',
        'AIRFLOW_VAR_DBT_TYPE': 'spark',
        'AIRFLOW_VAR_DBT_SPARK_METHOD': 'thrift',
        'AIRFLOW_VAR_DBT_SPARK_THRIFT_HOST': 'test.xyz',
        'AIRFLOW_VAR_DBT_SPARK_THRIFT_SCHEMA': 'ethereum',
        'AIRFLOW_VAR_DBT_SPARK_THRIFT_PORT': '',
        'AIRFLOW_VAR_DBT_SPARK_THRIFT_USER': '',
        'AIRFLOW_VAR_DBT_SPARK_THRIFT_AUTH': '',
        'AIRFLOW_VAR_DBT_SPARK_THRIFT_KERBEROS_SERVICE_NAME': '',
    })
    def test_spark_thrift_profile(self):
        with tempfile.TemporaryDirectory() as tempdir:
            profile = Profile()
            profile.dump_yml_file(profile_dir=tempdir)

            with open(os.path.join(tempdir, 'profiles.yml'), 'r') as f:
                expect_val = yaml.safe_load(_read_resource('spark_thrift_profiles.yml'))
                actual_val = yaml.safe_load(f.read())

                self.assertDictEqual(expect_val, actual_val)
