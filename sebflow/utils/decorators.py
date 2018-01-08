from copy import copy
from functools import wraps

import funcsigs

from sebflow.exceptions import SebflowException

signature = funcsigs.signature


def apply_defaults(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        dag_args = {}
        dag_params = {}
        if'dag' in kwargs:
            dag = kwargs['dag']
            dag_args = copy(dag.default_args) or {}
            dag_params = copy(dag_params) or {}

        params = {}
        if 'params' in kwargs:
            params = kwargs['params']

        dag_params.update(params)

        default_args = {}
        if 'default_args' in kwargs:
            default_args = kwargs['default_args']
            if 'params' in default_args:
                dag_params.update(default_args['params'])
                del default_args['params']

        dag_args.update(default_args)
        default_args = dag_args

        sig = signature(func)
        not_optional_args = [name for name, param in sig.parameters.items() if param.default == param.empty and param.name != 'self' and param.kind not in (param.VAR_POSITIONAL, param.VAR_KEYWORD)]
        for arg in sig.parameters:
            if arg in default_args and arg not in kwargs:
                kwargs[arg] = default_args[arg]
        missing_args = list(set(not_optional_args) - set(kwargs))
        if missing_args:
            raise SebflowException('Argument {0} is required'.format(missing_args))

        # kwargs['params'] = dag_params
        result = func(*args, **kwargs)
        return result
    return wrapper
