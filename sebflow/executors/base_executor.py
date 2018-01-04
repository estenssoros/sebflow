
class BaseExecutor(Object):
    def __init__(self):
        self.queued_tasks = {}
        self.running = {}
        self.event_buffer = {}

    def start(self):
        pass
