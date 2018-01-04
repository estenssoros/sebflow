# from sebflow import configuration
from sebflow.exceptions import SebFlowException
from sebflow.executors.local_executor import LocalExecutor
from sebflow.executors.sequential_executor import SequentialExecutor

DEFAULT_EXECUTOR = SequentialExecutor()


def GetDefaultExecutor():
    global DEFAULT_EXECUTOR
    if DEFAULT_EXECUTOR is not None:
        return DEFAULT_EXECUTOR

    executor_name = configuration.get('core', 'EXECUTOR')
    DEFAULT_EXECUTOR = _get_executor(executor_name)
    return DEFAULT_EXECUTOR


def _get_executor(executor_name):
    if executor_name == 'LocalExecutor':
        return LocalExecutor()
    elif executor_name == 'SequentialExecutor':
        return SequentialExecutor()
    else:
        raise SebFlowException
