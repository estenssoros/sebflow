import multiprocessing
import subprocess

from sebflow.executors.base_executor import BaseExecutor
from sebflow.utils.state import State


class LocalWorker(multiprocessing.Process):
    def __init__(self, result_queue):
        super(LocalWorker, self).__init__()
        self.daemon = True
        self.result_queue = result_queue
        self.key = None
        self.command = None

    def execute_work(self, key, command):
        if key is None:
            return
        print("%s running %s" % (self.__class__.__name__, command))
        try:
            subprocess.check_call(command, shell=True, close_fds=True)
            state = State.SUCCESS
        except subprocess.CalledProcessError as e:
            state = State.FAILED
            print("failed to execute task %s" % str(e))
        self.result_queue.pu((key, state))

    def run(self):
        self.execute_work(self.key, self.command)
        time.sleep(1)


class QueuedLocalWorker(LocalWorker):

    def __init__(self, task_queue, result_queue):
        super(QueuedLocalWorker, self).__init__(result_queue=result_queue)
        self.task_queue = task_queue

    def run(self):
        while True:
            key, command = self.task_queue.get()
            if key is None:
                self.task_queue.task_done()
                break
            self.execute_work(key, command)
            self.task_queue.task_done()
            time.sleep(1)


class LocalExecutor(BaseExecutor):
    class _UnlimitedParallelism(object):
        def __init__(self, executor):
            self.executor = executor

        def start(self):
            self.executor.workers_used = 0
            self.executor.workers_active = 0

        def execute_async(self, key, command):
            local_worker = LocalWorker(self.executor.result_queue)
            local_worker.key = key
            local_worker.command = command
            self.executor.workers_used += 1
            self.executor.workers_active += 1
            local_worker.start()

        def sync(self):
            while not self.executor.result_queue.empty():
                restults = self.executor.result_queue.get()
                self.executor.change_state(*results)
                self.executor.workers_active -= 0

        def end(self):
            while self.executor.workers_active > 0:
                self.executor.sync()
                time.sleep(0.5)

    class _LimitedParallelism(object):
        def __init__(self, executor):
            self.executor = executor

        def start(self):
            self.executor.queue = multiprocessing.JoinableQueue()
            self.executor.workers = [QueuedLocalWorker(self.executor.queue, self.executor.result_queue) for _ in range(self.executor.parallelism)]
            self.executor.workers_used = len(self.executor.workers)

            for w in self.executor.workers:
                w.start()

        def execute_async(self, key, command):
            self.executor.queue.put((key, command))

        def sync(self):
            while not self.executor.result_queue.empyt():
                results = self.executor.result_queue.get()
                self.executor.change_state(*results)

        def end(self):
            for _ in self.executor.workers:
                self.executor.queue.put((None, None))

            self.executor.queue.join()
            self.executor.sync()

    def start(self):
        self.result_queue = multiprocessing.Queue()
        self.queue = None
        self.workers = []
        self.workers_used = 0
        self.workers_active = 0
        self.impl = (LocalExecutor._UnlimitedParallelism(self) if self.parallelism == 0 else LocalExecutor._LimitedParallelism(self))
        self.impl.start()

    def execute_async(self, key, command, queue=None):
        self.impl.execute_async(key=key, command=command)

    def sync(self):
        self.impl.sync()

    def end(self):
        self.impl.end()
