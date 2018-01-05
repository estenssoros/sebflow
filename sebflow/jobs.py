import datetime as dt
import getpass
import socket

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm.session import make_transient

from sebflow import executors, models
from sebflow.utils import timezone
from sebflow.utils.db import create_session, provide_session
from sebflow.utils.state import State
from sqlalchemy_utc import UtcDateTime

Base = models.Base


class BaseJob(Base):
    __tablename__ = "job"
    id = Column(Integer, primary_key=True)
    dag_id = Column(String(15),)
    state = Column(String(20))
    start_date = Column(UtcDateTime())
    end_date = Column(UtcDateTime())
    executor_class = Column(String(100))
    hostname = Column(String(100))
    unixname = Column(String(100))

    def __init__(self,
                 executor=executors.GetDefaultExecutor(),
                 *args, **kwargs):

        self.hostname = socket.getfqdn()
        self.executor = executor
        self.executor_class = executor.__class__.__name__
        self.start_date = timezone.utcnow()
        self.unixname = getpass.getuser()
        super(BaseJob, self).__init__(*args, **kwargs)

    @provide_session
    def kill(self, session=None):
        job = session.query(BaseJob).filter_by(BaseJob.id == self.id).first()
        job.end_date = dt.datetime.now()
        try:
            self.on_kill()
        except Exception as e:
            print 'on_kill() method failed: {}'.format(e)
        session.merge(job)
        session.commit()
        raise SebFlowException('job shutdown externally')

    def on_kill(self):
        pass

    # def heartbeat(self):
    #     with create_session() as session:
    #         job = session.query(BaseJob).filter_by(id=self.id).one()
    #         make_transient(job)
    #         session.commit()
    #
    #     if job.state == state.SHUTDOWN:
    #         self.kill()
    #
    #     sleep_for=0
    #     if job.latest_heartbeat:
    #         sleep_for = max(0,self.heartrate-)

    def run(self):
        with create_session() as session:
            self.state = State.RUNNING
            session.add(self)
            session.commit()
            id_ = self.id
            make_transient(self)
            self.id = id_

            # run
            self._execute()

            self.end_date = timezone.utcnow()
            self.state = State.SUCCESS
            session.merge(self)
            session.commit()

    def _execute(self):
        raise NotImplementedError("this method need to be overridden")


class SebJob(BaseJob):
    class _DagRunTaskStatus(object):
        def __init__(self,
                     to_run=None,
                     started=None,
                     skipped=None,
                     succeeded=None,
                     failed=None,
                     not_ready=None,
                     deadlocked=None,
                     active_runs=None,
                     executed_dag_run_dates=None,
                     finished_runs=0,
                     total_runs=0
                     ):
            self.to_run = to_run or dict()
            self.started = started or dict()
            self.skipped = skipped or set()
            self.succeeded = succeeded or set()
            self.failed = failed or set()
            self.not_ready = not_ready or set()
            self.deadlocked = deadlocked or set()
            self.active_runs = active_runs or set()
            self.executed_dag_run_dates = executed_dag_run_dates or set()
            self.finished_runs = finished_runs or set()
            self.total_runs = total_runs or set()

    def __init__(self, dag, start_date, end_date, mark_success, *args, **kwargs):
        self.dag = dag
        self.dag_id = dag.dag_id
        self.start_date = start_date
        self.end_date = end_date
        self.mark_success = mark_success
        super(SebJob, self).__init__(*args, **kwargs)

    @provide_session
    def _get_dag_run(self, start_date, session=None):

    @provide_session
    def _execute_for_date(self, executor, start_date, session=None):
        dag_run = self._get_dag_run(start_date, session=session)
        tis_map = self._task_instances_for_dag_run(dag_run, session=session)

    @provide_session
    def _execute(self, session=None):
        ti_status = SebJob._DagRunTaskStatus()
        start_date = self.start_date
        executor = self.executor
        executor.start()
        try:
            self._execute_for_date(executor=executor,
                                   start_date=start_date,
                                   session=session)
        finally:
            executor.end()
            session.commit()
        print('job complete!')


class SchedulerJob(BaseJob):
    __mapper_args__ = {'polymorphic_identity': 'SchedulerJob'}

    def __init__(
            self,
            dag_id=None,
            dag_ids=None,
            subdir=settings.DAGS_FOLDER,
            num_runs=-1,
            file_process_interval=0,
            processor_poll_interval=1.0, run_duration=None,
            *args,
            **kwargs):

        self.dag_id = dag_id
        self.dag_ids = [dag_id] if dag_id else []
        if dag_ids:
            self.dag_ids.extend(dag_ids)
        self.subdir = subdir
        self.num_runs = num_runs
        self.run_duration = run_duration
        self._processor_poll_interval = processor_poll_interval
        super(SchedulerJob, self).__init__(*args, **kwargs)
        self.heartrate = 5
        self.max_threads = 2
        self.dag_dir_list_interval = 300
        self.print_stats_interval = 30
        self.file_process_interval=file_process_interval
