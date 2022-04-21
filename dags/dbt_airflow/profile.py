import json
import logging
import os
from typing import TypeVar

import pyaml

from dbt_airflow.credential import Credential, CredentialFactory
from variables import read_vars, parse_int

C = TypeVar('C', bound=Credential)


class Profile:
    profile_name: str
    target_name: str
    threads: int
    credential: C
    logger: logging.Logger

    def __init__(self, prefix: str = 'dbt_'):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.profile_name = read_vars('profile_name', var_prefix=prefix, required=True)
        self.target_name = read_vars('target_name', var_prefix=prefix, required=True)
        self.threads = parse_int(read_vars('thread', var_prefix=prefix, default='4'))
        self.credential = CredentialFactory.new_creds(prefix)

    def dump_yml_file(self, profile_dir: str):
        if not os.path.exists(profile_dir) or not os.path.isdir(profile_dir):
            raise Exception(f'{profile_dir} is not exists or is not a dir.')

        profile_dict = {
            self.profile_name: {
                'target': self.target_name,
                'outputs': {
                    self.target_name: {
                        'type': self.credential.type,
                        'threads': self.threads,
                        **{k: v for k, v in self.credential.__dict__.items() if v is not None}
                    }
                }
            }
        }

        self.logger.info('dbt profile configs: ')
        self.logger.info(json.dumps(profile_dict, indent=4, sort_keys=True))

        with open(os.path.join(profile_dir, 'profiles.yml'), 'w') as f:
            f.write(pyaml.dump(profile_dict, sort_dicts=False))
