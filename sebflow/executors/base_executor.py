from sebflow.log.logging_mixin import LoggingMixin


class BaseExecutor(LoggingMixin):
    def __init__(self, dag, parallelism=32):
        self.dag = dag
        self.parallelism = parallelism
        self.queued_tasks = {}
        self.running = {}
        self.event_buffer = {}

    def start(self):
        pass

    def queue_task(self, task):
        pass

    def has_task(self, task):
        if task.task_id in self.queued_tasks or task.task_id in self.running:
            return True
        return False

    def end(self):
        raise NotImplementedError()
