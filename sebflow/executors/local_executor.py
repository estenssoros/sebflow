import getpass
import multiprocessing
import socket
import time
from Queue import Empty as QueueEmpty

from sebflow import timezone
from sebflow.executors.base_executor import BaseExecutor
from sebflow.models import DagModel, DagRun, Task
from sebflow.state import State
from sebflow.utils.db import provide_session
from sebflow.utils.log.logging_mixin import LoggingMixin


class LocalWorker(multiprocessing.Process, LoggingMixin):
    """
    LocalWorker Process implementation. Executes the given command and puts the
    result into a result queue when done, terminating execution.
    """

    def __init__(self, result_queue):
        super(LocalWorker, self).__init__()
        self.daemon = False
        self.result_queue = result_queue

    def run(self):
        raise self.NotImplementedError()


class QueuedLocalWorker(LocalWorker):
    """
    LocalWorker implementation that is waiting for tasks from a queue and will
    continue executing commands as they become available in the queue. It will terminate
    execution once None is sent throught the quque.
    """

    def __init__(self, task_queue, eval_queue, result_queue):
        super(QueuedLocalWorker, self).__init__(result_queue=result_queue)
        self.task_queue = task_queue
        self.eval_queue = eval_queue

    @provide_session
    def run(self, session=None):
        while True:
            try:
                task = self.task_queue.get_nowait()
                if task is None:
                    break
            except QueueEmpty:
                time.sleep(1)
                continue

            # HOLD
            if task.state != State.QUEUED:
                self.eval_queue.put(task)
                continue

            # EXECUTE
            try:
                task.execute()
                task.state = State.SUCCESS
                self.logger.info('task %s [successful]' % task.task_id)
                msg = None
            except Exception as e:  # return error message somewhere, somehow
                task.state = State.FAILED
                task.message = str(e)
                msg = e
                self.logger.error('task %s [failed]' % task.task_id)

            self.eval_queue.put(task)
            self.result_queue.put((task.task_id, task.state, msg))


class Evaluator(multiprocessing.Process, LoggingMixin):
    """
    Helper class to run QueuedLocalWorker. Takse the place of Scheduler for
    evaulating and marking tasks as complete, queued, failed, etc.
    """

    def __init__(self, dag_info, task_dict, task_queue, eval_queue, result_queue):
        self.dag_id = dag_info['dag_id']
        self.dag_run_id = dag_info['dag_run_id']
        self.task_dict = task_dict

        self.task_queue = task_queue
        self.eval_queue = eval_queue
        self.result_queue = result_queue

        self.success = set()
        self.failed = set()
        self.upstream_failed = set()
        self._upstream_failed = set()

        self.lists = (
            self.success,
            self.failed,
            self.upstream_failed
        )
        super(Evaluator, self).__init__()

    @property
    def done(self):
        return sum(len(l) for l in self.lists) == len(self.task_dict)

    @provide_session
    def run(self, session=None):
        while True:
            try:
                task = self.eval_queue.get_nowait()
            except QueueEmpty:
                if self.done:
                    break
                time.sleep(0.5)
                continue

            if task.state == State.QUEUED:
                task_model = session.query(Task).filter_by(task_id=task.task_id, dag_run_id=self.dag_run_id).first()
                task_model.state = State.QUEUED
                task_model.start_date = timezone.utcnow()
                session.commit()
                self.task_queue.put(task)
                continue

            if task.state == State.SUCCESS:
                task_model = session.query(Task).filter_by(task_id=task.task_id, dag_run_id=self.dag_run_id).first()
                task_model.state = State.SUCCESS
                task_model.end_date = timezone.utcnow()
                session.commit()
                self.success.add(task.task_id)
                continue

            if task.state == State.FAILED:
                task_model = session.query(Task).filter_by(task_id=task.task_id, dag_run_id=self.dag_run_id).first()
                task_model.state = State.FAILED
                task_model.end_date = timezone.utcnow()
                task_model.message = task.message
                session.commit()
                self.failed.add(task.task_id)
                for task_id in task.downstream_task_ids:
                    self._upstream_failed.add(task_id)
                continue

            # CHECK PENDING
            if task.state == State.PENDING:

                if task.task_id in self._upstream_failed:
                    task.state = State.UPSTREAM_FAILED
                    task_model = session.query(Task).filter_by(task_id=task.task_id, dag_run_id=self.dag_run_id).first()
                    task_model.state = State.UPSTREAM_FAILED
                    task_model.end_date = timezone.utcnow()
                    session.commit()
                    self.upstream_failed.add(task.task_id)
                    self.logger.debug('task %s [upstream_failed]' % task.task_id)

                    for tid in task.downstream_task_ids:
                        self._upstream_failed.add(tid)

                    self.result_queue.put((task.task_id, State.UPSTREAM_FAILED, None))
                    continue

                if all(tid in self.success for tid in task.upstream_task_ids):
                    task.state = State.QUEUED
                    task_model = session.query(Task).filter_by(task_id=task.task_id, dag_run_id=self.dag_run_id).first()
                    task_model.state = State.QUEUED
                    task_model.start_date = timezone.utcnow()
                    session.commit()
                    session.commit()
                    self.task_queue.put(task)
                    continue
                    
            if task.state == State.UPSTREAM_FAILED:
                self.result_queue.put((task.task_id, State.UPSTREAM_FAILED, None))
                continue

            # FINALLY
            self.eval_queue.put(task)

        dag_run = session.query(DagRun).filter_by(dag_run_id=self.dag_run_id).first()
        dag_run.end_date = timezone.utcnow()
        dag_run.state = State.SUCCESS
        session.commit()

        dag = session.query(DagModel).filter_by(dag_id=self.dag_id).first()
        if len(self.success) == len(self.task_dict):
            dag.last_run_result = State.SUCCESS
        else:
            dag.last_run_result = State.FAILED
        session.commit()


class LocalExecutor(BaseExecutor):
    """
    LocalExecutor executes tasks locally in parallel. It uses the
    multiprocessing Python library and queues to parallelize the execution
    of tasks.
    """
    class _LimitedParallelism(LoggingMixin):
        """
        Implements LocalExecutor with limited parallelism using a task queue to
        coordinate work distribution.
        """

        def __init__(self, executor):
            self.executor = executor

        def start(self):
            self.executor.queue = multiprocessing.Queue()

            self.executor.workers = [
                QueuedLocalWorker(self.executor.queue, self.executor.eval_queue, self.executor.result_queue)
                for _ in range(min((len(self.executor.dag.tasks), self.executor.parallelism)))
            ]

            self.executor.workers_used = len(self.executor.workers)
            self.logger.info('LocalExecutor starting %d workers' % self.executor.workers_used)
            for w in self.executor.workers:
                w.start()

        def sync(self):
            '''
            occurs after end
            '''
            while not self.executor.result_queue.empty():
                task_id, state, msg = self.executor.result_queue.get()
                self.executor.dag.get_task(task_id).state = state
                self.executor.dag.get_task(task_id).msg = msg

        def end(self):
            '''
            kill workers and evaluator
            '''
            for _ in self.executor.workers:
                self.executor.queue.put(None)
                self.executor.sync()

    def get_task_dict(self):
        task_dict = {}
        for task in self.dag.tasks:
            task_dict[task.task_id] = task.downstream_task_ids
        return task_dict

    @provide_session
    def start(self, session=None):

        self.result_queue = multiprocessing.Queue()
        self.eval_queue = multiprocessing.Queue()

        self.queue = None  # gets initiated in _LimitedParallelism
        self.workers = []
        self.workers_used = 0
        self.workers_active = 0
        self.impl = LocalExecutor._LimitedParallelism(self)
        self.impl.start()

        task_dict = self.get_task_dict()
        dag_info = {'dag_id': self.dag.dag_id, 'dag_run_id': self.dag.dag_run_id}
        self.evalulator = Evaluator(dag_info, task_dict, self.queue, self.eval_queue, self.result_queue)
        self.evalulator.start()

        for task in self.dag.tasks:
            if task in self.dag.roots:
                task.state = State.QUEUED
            task_model = Task(task_id=task.task_id,
                              dag_run_id=self.dag.dag_run_id,
                              state=task.state,
                              hostname=socket.getfqdn(),
                              unixname=getpass.getuser()
                              )
            session.add(task_model)
            session.commit()
            self.eval_queue.put(task)

    def sync(self):
        self.impl.sync()

    def end(self):
        self.logger.info('waiting for evaluator to finish')
        self.evalulator.join()
        self.impl.end()
