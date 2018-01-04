import datetime as dt
import getpass
import socket

from sebflow import models
from sebflow.utils import timezone
from sebflow.utils.db import create_session, provide_session
from sebflow.utils.state import State
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm.session import make_transient
from sqlalchemy_utc import UtcDateTime

Base = models.Base


class BaseJob(Base):
    __tablename__ = "job"
    id = Column(Integer, primary_key=True)
    dag_id = Column(String(15),)
    state = Column(String(20))
    start_date = Column(UtcDateTime())
    end_date = Column(UtcDateTime())
    hostname = Column(String(100))
    unixname = Column(String(100))

    def __init__(self, *args, **kwargs):
        self.start_date = timezone.utcnow()
        self.hostname = socket.getfqdn()
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


class SchedulerJob(BaseJob):
    def __init__(self, dag, start_date, end_date, mark_success, *args, **kwargs):
        self.dag = dag
        self.dag_id = dag.dag_id
        self.start_date = start_date
        self.end_date = end_date
        self.mark_success = mark_success
        super(SebJob, self).__init__(*args, **kwargs)

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


    @provide_session
    def _execute(self, session=None):
