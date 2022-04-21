from dataclasses import dataclass
from typing import Optional, TypeVar

from dbt.adapters.spark.connections import SparkConnectionMethod

from variables import read_vars, parse_int

C = TypeVar('C', bound='Credential')


@dataclass
class Credential:
    schema: str
    type: str
    database: Optional[str]


@dataclass
class SparkCredential(Credential):
    method: str


@dataclass
class SparkThriftCredential(SparkCredential):
    host: str
    port: int
    user: Optional[str]
    auth: Optional[str]
    kerberos_service_name: Optional[str]


class CredentialFactory:

    @staticmethod
    def new_creds(prefix: str) -> C:
        type = read_vars('type', var_prefix=prefix, default='spark')

        if type == 'spark':
            return CredentialFactory._new_spark_creds(prefix=prefix + 'spark_')

    @staticmethod
    def _new_spark_creds(prefix: str) -> C:
        method = read_vars('method', var_prefix=prefix, required=True)
        if method == SparkConnectionMethod.THRIFT.value:
            prefix = prefix + 'thrift_'
            host = read_vars('host', var_prefix=prefix, required=True)
            schema = read_vars('schema', var_prefix=prefix, required=True)
            port = parse_int(read_vars('port', var_prefix=prefix, default='10001'))
            user = read_vars('user', var_prefix=prefix)
            auth = read_vars('auth', var_prefix=prefix)
            kerberos_service_name = read_vars('kerberos_service_name', var_prefix=prefix)
            return SparkThriftCredential(
                host=host, method=method, database=None, schema=schema, port=port, user=user,
                auth=auth, kerberos_service_name=kerberos_service_name, type='spark')
        else:
            raise ValueError(f'{method} connection in the spark is not be supported now.')
