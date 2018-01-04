import subprocess

from sebflow.executors.base_executor import BaseExecutor
from sebflow.utils.state import State


class SequentialExecutor(BaseExecutor):
    def __init__(self):
        super(SequentialExecutor, self).__init__()
        self.commands_to_run = []

    def execute_async(self, key, command, queue=None):
        self.commands_to_run.append((key, command,))

    def sync(self):
        for key, command in self.commands_to_run:
            print ('executing command %s' % command)
        try:
            subprocess.check_call(command, shell=True, close_fds=True)
            self.change_state(key, State.SUCCESS)
        except subprocess.CalledProcessError as e:
            self.change_state(key, State.FAILED)
            print('failed to execute task %s' % str(e))
        self.commands_to_run = []

    def end(self):
        self.heartbeat()
