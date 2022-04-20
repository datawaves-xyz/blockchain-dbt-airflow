import json
import logging
import os.path

import pyaml
from dbt.adapters.spark import SparkCredentials
from dbt.adapters.spark.connections import SparkConnectionMethod
from dbt.config import Profile
from dbt.contracts.project import UserConfig

from dags.variables import read_vars, parse_int


class ProfileWrapper:
    _profile: Profile
    _logger: logging.Logger

    def __init__(self):
        self._profile = self._new_profile('dbt_')
        self._logger = logging.getLogger(self.__class__.__name__)

    def _new_profile(self, prefix: str) -> Profile:
        profile_name = read_vars('profile_name', var_prefix=prefix, required=True)
        target_name = read_vars('target_name', var_prefix=prefix, required=True)
        threads = parse_int(read_vars('thread', var_prefix=prefix, default='4'))
        dtype = read_vars('type', var_prefix=prefix, default='spark')

        if dtype == 'spark':
            creds = self._new_spark_credentials(prefix=prefix + 'spark_')
        else:
            raise ValueError(f'{dtype} database is not be supported now.')

        return Profile(profile_name=profile_name, target_name=target_name, user_config=UserConfig(), threads=threads,
                       credentials=creds)

    @staticmethod
    def _new_spark_credentials(prefix: str) -> SparkCredentials:
        method = SparkConnectionMethod(read_vars('method', var_prefix=prefix, required=True))
        if method == SparkConnectionMethod.THRIFT:
            prefix = prefix + 'thrift_'
            host = read_vars('host', var_prefix=prefix, required=True)
            schema = read_vars('schema', var_prefix=prefix, required=True)
            port = parse_int(read_vars('port', var_prefix=prefix, default='10001'))
            user = read_vars('user', var_prefix=prefix)
            auth = read_vars('auth', var_prefix=prefix)
            kerberos_service_name = read_vars('kerberos_service_name', var_prefix=prefix)
            return SparkCredentials(host=host, method=method, database=None, schema=schema, port=port, user=user,
                                    auth=auth, kerberos_service_name=kerberos_service_name)
        else:
            raise ValueError(f'{method} connection in the spark is not be supported now.')

    def to_yml_file(self, profile_dir: str):
        if not os.path.exists(profile_dir) or not os.path.isdir(profile_dir):
            raise Exception(f'{profile_dir} is not exists or is not a dir.')

        profile_dict = {
            self._profile.profile_name: {
                'target': self._profile.target_name,
                'outputs': {
                    self._profile.target_name: {
                        'type': self._profile.credentials.type,
                        'threads': self._profile.threads,
                        **self._profile.credentials
                    }
                }
            }
        }

        self._logger.info('dbt profile configs: ')
        self._logger.info(json.dumps(profile_dict, indent=4, sort_keys=True))

        with open(os.path.join(profile_dir, 'profiles.yml'), 'w') as f:
            f.write(pyaml.dump(profile_dict, sort_dicts=False))
