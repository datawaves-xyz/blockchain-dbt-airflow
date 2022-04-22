import json
from typing import Optional, Dict

from airflow.models import Variable


def read_vars(
        var_name: str,
        var_prefix: Optional[str] = None,
        required: bool = False,
        default: Optional[str] = None,
        **kwargs: Dict[str, any]
) -> str:
    full_var_name = var_name if var_prefix is None else f'{var_prefix}{var_name}'
    var = Variable.get(full_var_name, '')
    if var == '':
        var = None
    if var is None:
        var = default
    if var is None:
        var = kwargs.get(full_var_name)
    if required and var is None:
        raise ValueError(f'{full_var_name} variable is required.')
    return var


def parse_bool(
        bool_string: Optional[str], default: bool = True
) -> bool:
    if isinstance(bool_string, bool):
        return bool_string
    if bool_string is None or len(bool_string) == 0:
        return default
    else:
        return bool_string.lower() in ['true', 'yes']


def parse_int(
        val: Optional[str]
) -> Optional[int]:
    if val is None:
        return None
    return int(val)


def parse_dict(
        val: Optional[str]
) -> Optional[Dict[any, any]]:
    if val is None:
        return None
    return json.loads(val)
