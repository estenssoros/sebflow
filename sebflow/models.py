import copy
import functools
import getpass
import hashlib
import json
import logging
import os
import sys
import six
import warnings
from datetime import datetime, timedelta

from sqlalchemy import (Boolean, Column, DateTime, Float, ForeignKey, Index,
                        Integer, LargeBinary, PickleType, String, Text, func)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import reconstructor
from sqlalchemy_utc import UtcDateTime

from sebflow import configuration, settings
from sebflow.exceptions import SebflowException
from sebflow.executors import GetDefaultExecutor, LocalExecutor
from sebflow.utils import timezone
from sebflow.utils.dates import cron_presets
from sebflow.utils.db import provide_session
from sebflow.utils.decorators import apply_defaults
from sebflow.utils.helpers import validate_key
from sebflow.utils.log.logging_mixin import LoggingMixin
from sebflow.utils.operator_resources import Resources
from sebflow.utils.state import State
from sebflow.utils.trigger_rule import TriggerRule

Base = declarative_base()
ID_LEN = 250
XCOM_RETURN_KEY = 'return_value'

_CONTEXT_MANAGER_DAG = None


class TaskInstance(Base, LoggingMixin):
    __tablename__ = "task_instance"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    execution_date = Column(UtcDateTime, primary_key=True)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    duration = Column(Float)
    state = Column(String(20))
    try_number = Column(Integer, default=0)
    max_tries = Column(Integer)
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    job_id = Column(Integer)
    pool = Column(String(50))
    queue = Column(String(50))
    priority_weight = Column(Integer)
    operator = Column(String(1000))
    queued_dttm = Column(UtcDateTime)
    pid = Column(Integer)

    __table_args__ = (
        Index('ti_dag_state', dag_id, state),
        Index('ti_state', state),
        Index('ti_state_lkp', dag_id, task_id, execution_date, state),
        Index('ti_pool', pool, state, priority_weight),
    )

    def __init__(self, task, execution_date, state=None):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.execution_date = execution_date
        self.task = task
        self.queue = task.queue
        self.pool = task.pool
        self.priority_weight = task.priority_weight_total
        self.try_number = 0
        self.max_tries = self.task.retries
        self.unixname = getpass.getuser()
        self.run_as_user = task.run_as_user
        if state:
            self.state = state
        self.hostname = ''
        self.init_on_load()
        self._log = logging.getLogger("airflow.task")

    @reconstructor
    def init_on_load(self):
        """ Initialize the attributes that aren't stored in the DB. """
        self.test_mode = False  # can be changed when calling 'run'

    def command(
            self,
            mark_success=False,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            local=False,
            pickle_id=None,
            raw=False,
            job_id=None,
            pool=None,
            cfg_path=None):
        """
        Returns a command that can be executed anywhere where airflow is
        installed. This command is part of the message sent to executors by
        the orchestrator.
        """
        return " ".join(self.command_as_list(
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            local=local,
            pickle_id=pickle_id,
            raw=raw,
            job_id=job_id,
            pool=pool,
            cfg_path=cfg_path))

    def command_as_list(
            self,
            mark_success=False,
            ignore_all_deps=False,
            ignore_task_deps=False,
            ignore_depends_on_past=False,
            ignore_ti_state=False,
            local=False,
            pickle_id=None,
            raw=False,
            job_id=None,
            pool=None,
            cfg_path=None):
        """
        Returns a command that can be executed anywhere where airflow is
        installed. This command is part of the message sent to executors by
        the orchestrator.
        """
        dag = self.task.dag

        should_pass_filepath = not pickle_id and dag
        if should_pass_filepath and dag.full_filepath != dag.filepath:
            path = "DAGS_FOLDER/{}".format(dag.filepath)
        elif should_pass_filepath and dag.full_filepath:
            path = dag.full_filepath
        else:
            path = None

        return TaskInstance.generate_command(
            self.dag_id,
            self.task_id,
            self.execution_date,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_ti_state=ignore_ti_state,
            local=local,
            pickle_id=pickle_id,
            file_path=path,
            raw=raw,
            job_id=job_id,
            pool=pool,
            cfg_path=cfg_path)

    @staticmethod
    def generate_command(dag_id,
                         task_id,
                         execution_date,
                         mark_success=False,
                         ignore_all_deps=False,
                         ignore_depends_on_past=False,
                         ignore_task_deps=False,
                         ignore_ti_state=False,
                         local=False,
                         pickle_id=None,
                         file_path=None,
                         raw=False,
                         job_id=None,
                         pool=None,
                         cfg_path=None
                         ):
        """
        Generates the shell command required to execute this task instance.

        :param dag_id: DAG ID
        :type dag_id: unicode
        :param task_id: Task ID
        :type task_id: unicode
        :param execution_date: Execution date for the task
        :type execution_date: datetime
        :param mark_success: Whether to mark the task as successful
        :type mark_success: bool
        :param ignore_all_deps: Ignore all ignorable dependencies.
            Overrides the other ignore_* parameters.
        :type ignore_all_deps: boolean
        :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs
            (e.g. for Backfills)
        :type ignore_depends_on_past: boolean
        :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past
            and trigger rule
        :type ignore_task_deps: boolean
        :param ignore_ti_state: Ignore the task instance's previous failure/success
        :type ignore_ti_state: boolean
        :param local: Whether to run the task locally
        :type local: bool
        :param pickle_id: If the DAG was serialized to the DB, the ID
            associated with the pickled DAG
        :type pickle_id: unicode
        :param file_path: path to the file containing the DAG definition
        :param raw: raw mode (needs more details)
        :param job_id: job ID (needs more details)
        :param pool: the Airflow pool that the task should run in
        :type pool: unicode
        :return: shell command that can be used to run the task instance
        """
        iso = execution_date.isoformat()
        cmd = ["airflow", "run", str(dag_id), str(task_id), str(iso)]
        cmd.extend(["--mark_success"]) if mark_success else None
        cmd.extend(["--pickle", str(pickle_id)]) if pickle_id else None
        cmd.extend(["--job_id", str(job_id)]) if job_id else None
        cmd.extend(["-A"]) if ignore_all_deps else None
        cmd.extend(["-i"]) if ignore_task_deps else None
        cmd.extend(["-I"]) if ignore_depends_on_past else None
        cmd.extend(["--force"]) if ignore_ti_state else None
        cmd.extend(["--local"]) if local else None
        cmd.extend(["--pool", pool]) if pool else None
        cmd.extend(["--raw"]) if raw else None
        cmd.extend(["-sd", file_path]) if file_path else None
        cmd.extend(["--cfg_path", cfg_path]) if cfg_path else None
        return cmd

    @property
    def log_filepath(self):
        iso = self.execution_date.isoformat()
        log = os.path.expanduser(configuration.get('core', 'BASE_LOG_FOLDER'))
        return (
            "{log}/{self.dag_id}/{self.task_id}/{iso}.log".format(**locals()))

    @property
    def log_url(self):
        iso = self.execution_date.isoformat()
        BASE_URL = configuration.get('webserver', 'BASE_URL')
        return BASE_URL + (
            "/admin/airflow/log"
            "?dag_id={self.dag_id}"
            "&task_id={self.task_id}"
            "&execution_date={iso}"
        ).format(**locals())

    @property
    def mark_success_url(self):
        iso = self.execution_date.isoformat()
        BASE_URL = configuration.get('webserver', 'BASE_URL')
        return BASE_URL + (
            "/admin/airflow/action"
            "?action=success"
            "&task_id={self.task_id}"
            "&dag_id={self.dag_id}"
            "&execution_date={iso}"
            "&upstream=false"
            "&downstream=false"
        ).format(**locals())

    @provide_session
    def current_state(self, session=None):
        """
        Get the very latest state from the database, if a session is passed,
        we use and looking up the state becomes part of the session, otherwise
        a new session is used.
        """
        TI = TaskInstance
        ti = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date == self.execution_date,
        ).all()
        if ti:
            state = ti[0].state
        else:
            state = None
        return state

    @provide_session
    def error(self, session=None):
        """
        Forces the task instance's state to FAILED in the database.
        """
        self.log.error("Recording the task instance as FAILED")
        self.state = State.FAILED
        session.merge(self)
        session.commit()

    @provide_session
    def refresh_from_db(self, session=None, lock_for_update=False):
        """
        Refreshes the task instance from the database based on the primary key

        :param lock_for_update: if True, indicates that the database should
            lock the TaskInstance (issuing a FOR UPDATE clause) until the
            session is committed.
        """
        TI = TaskInstance

        qry = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date == self.execution_date)

        if lock_for_update:
            ti = qry.with_for_update().first()
        else:
            ti = qry.first()
        if ti:
            self.state = ti.state
            self.start_date = ti.start_date
            self.end_date = ti.end_date
            self.try_number = ti.try_number
            self.max_tries = ti.max_tries
            self.hostname = ti.hostname
            self.pid = ti.pid
        else:
            self.state = None

    @provide_session
    def clear_xcom_data(self, session=None):
        """
        Clears all XCom data from the database for the task instance
        """
        session.query(XCom).filter(
            XCom.dag_id == self.dag_id,
            XCom.task_id == self.task_id,
            XCom.execution_date == self.execution_date
        ).delete()
        session.commit()

    @property
    def key(self):
        """
        Returns a tuple that identifies the task instance uniquely
        """
        return self.dag_id, self.task_id, self.execution_date

    @provide_session
    def set_state(self, state, session=None):
        self.state = state
        self.start_date = timezone.utcnow()
        self.end_date = timezone.utcnow()
        session.merge(self)
        session.commit()

    @property
    def is_premature(self):
        """
        Returns whether a task is in UP_FOR_RETRY state and its retry interval
        has elapsed.
        """
        # is the task still in the retry waiting period?
        return self.state == State.UP_FOR_RETRY and not self.ready_for_retry()

    @provide_session
    def are_dependents_done(self, session=None):
        """
        Checks whether the dependents of this task instance have all succeeded.
        This is meant to be used by wait_for_downstream.

        This is useful when you do not want to start processing the next
        schedule of a task until the dependents are done. For instance,
        if the task DROPs and recreates a table.
        """
        task = self.task

        if not task.downstream_task_ids:
            return True

        ti = session.query(func.count(TaskInstance.task_id)).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id.in_(task.downstream_task_ids),
            TaskInstance.execution_date == self.execution_date,
            TaskInstance.state == State.SUCCESS,
        )
        count = ti[0][0]
        return count == len(task.downstream_task_ids)

    @property
    @provide_session
    def previous_ti(self, session=None):
        """ The task instance for the task that ran before this task instance """

        dag = self.task.dag
        if dag:
            dr = self.get_dagrun(session=session)

            # LEGACY: most likely running from unit tests
            if not dr:
                # Means that this TI is NOT being run from a DR, but from a catchup
                previous_scheduled_date = dag.previous_schedule(self.execution_date)
                if not previous_scheduled_date:
                    return None

                return TaskInstance(task=self.task,
                                    execution_date=previous_scheduled_date)

            dr.dag = dag
            if dag.catchup:
                last_dagrun = dr.get_previous_scheduled_dagrun(session=session)
            else:
                last_dagrun = dr.get_previous_dagrun(session=session)

            if last_dagrun:
                return last_dagrun.get_task_instance(self.task_id, session=session)

        return None

    @provide_session
    def are_dependencies_met(
            self,
            dep_context=None,
            session=None,
            verbose=False):
        """
        Returns whether or not all the conditions are met for this task instance to be run
        given the context for the dependencies (e.g. a task instance being force run from
        the UI will ignore some dependencies).

        :param dep_context: The execution context that determines the dependencies that
            should be evaluated.
        :type dep_context: DepContext
        :param session: database session
        :type session: Session
        :param verbose: whether or not to print details on failed dependencies
        :type verbose: boolean
        """
        dep_context = dep_context or DepContext()
        failed = False
        for dep_status in self.get_failed_dep_statuses(
                dep_context=dep_context,
                session=session):
            failed = True
            if verbose:
                self.log.info(
                    "Dependencies not met for %s, dependency '%s' FAILED: %s",
                    self, dep_status.dep_name, dep_status.reason
                )

        if failed:
            return False

        if verbose:
            self.log.info("Dependencies all met for %s", self)

        return True

    @provide_session
    def get_failed_dep_statuses(
            self,
            dep_context=None,
            session=None):
        dep_context = dep_context or DepContext()
        for dep in dep_context.deps | self.task.deps:
            for dep_status in dep.get_dep_statuses(
                    self,
                    session,
                    dep_context):

                self.log.debug(
                    "%s dependency '%s' PASSED: %s, %s",
                    self, dep_status.dep_name, dep_status.passed, dep_status.reason
                )

                if not dep_status.passed:
                    yield dep_status

    def __repr__(self):
        return (
            "<TaskInstance: {ti.dag_id}.{ti.task_id} "
            "{ti.execution_date} [{ti.state}]>"
        ).format(ti=self)

    def next_retry_datetime(self):
        """
        Get datetime of the next retry if the task instance fails. For exponential
        backoff, retry_delay is used as base and will be converted to seconds.
        """
        delay = self.task.retry_delay
        if self.task.retry_exponential_backoff:
            min_backoff = int(delay.total_seconds() * (2 ** (self.try_number - 2)))
            # deterministic per task instance
            hash = int(hashlib.sha1("{}#{}#{}#{}".format(self.dag_id, self.task_id,
                                                         self.execution_date, self.try_number).encode('utf-8')).hexdigest(), 16)
            # between 0.5 * delay * (2^retry_number) and 1.0 * delay * (2^retry_number)
            modded_hash = min_backoff + hash % min_backoff
            # timedelta has a maximum representable value. The exponentiation
            # here means this value can be exceeded after a certain number
            # of tries (around 50 if the initial delay is 1s, even fewer if
            # the delay is larger). Cap the value here before creating a
            # timedelta object so the operation doesn't fail.
            delay_backoff_in_seconds = min(
                modded_hash,
                timedelta.max.total_seconds() - 1
            )
            delay = timedelta(seconds=delay_backoff_in_seconds)
            if self.task.max_retry_delay:
                delay = min(self.task.max_retry_delay, delay)
        return self.end_date + delay

    def ready_for_retry(self):
        """
        Checks on whether the task instance is in the right state and timeframe
        to be retried.
        """
        return (self.state == State.UP_FOR_RETRY and
                self.next_retry_datetime() < timezone.utcnow())

    @provide_session
    def pool_full(self, session):
        """
        Returns a boolean as to whether the slot pool has room for this
        task to run
        """
        if not self.task.pool:
            return False

        pool = (
            session
            .query(Pool)
            .filter(Pool.pool == self.task.pool)
            .first()
        )
        if not pool:
            return False
        open_slots = pool.open_slots(session=session)

        return open_slots <= 0

    @provide_session
    def get_dagrun(self, session):
        """
        Returns the DagRun for this TaskInstance

        :param session:
        :return: DagRun
        """
        dr = session.query(DagRun).filter(
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date == self.execution_date
        ).first()

        return dr

    @provide_session
    def _check_and_change_state_before_execution(
            self,
            verbose=True,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            mark_success=False,
            test_mode=False,
            job_id=None,
            pool=None,
            session=None):
        """
        Checks dependencies and then sets state to RUNNING if they are met. Returns
        True if and only if state is set to RUNNING, which implies that task should be
        executed, in preparation for _run_raw_task

        :param verbose: whether to turn on more verbose logging
        :type verbose: boolean
        :param ignore_all_deps: Ignore all of the non-critical dependencies, just runs
        :type ignore_all_deps: boolean
        :param ignore_depends_on_past: Ignore depends_on_past DAG attribute
        :type ignore_depends_on_past: boolean
        :param ignore_task_deps: Don't check the dependencies of this TI's task
        :type ignore_task_deps: boolean
        :param ignore_ti_state: Disregards previous task instance state
        :type ignore_ti_state: boolean
        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: boolean
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: boolean
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        :return: whether the state was changed to running or not
        :rtype: bool
        """
        task = self.task
        self.pool = pool or task.pool
        self.test_mode = test_mode
        self.refresh_from_db(session=session, lock_for_update=True)
        self.job_id = job_id
        self.hostname = socket.getfqdn()
        self.operator = task.__class__.__name__

        if not ignore_all_deps and not ignore_ti_state and self.state == State.SUCCESS:
            Stats.incr('previously_succeeded', 1, 1)

        queue_dep_context = DepContext(
            deps=QUEUE_DEPS,
            ignore_all_deps=ignore_all_deps,
            ignore_ti_state=ignore_ti_state,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps)
        if not self.are_dependencies_met(
                dep_context=queue_dep_context,
                session=session,
                verbose=True):
            session.commit()
            return False

        # TODO: Logging needs cleanup, not clear what is being printed
        hr = "\n" + ("-" * 80) + "\n"  # Line break

        # For reporting purposes, we report based on 1-indexed,
        # not 0-indexed lists (i.e. Attempt 1 instead of
        # Attempt 0 for the first attempt).
        msg = "Starting attempt {attempt} of {total}".format(
            attempt=self.try_number + 1,
            total=self.max_tries + 1)
        self.start_date = timezone.utcnow()

        dep_context = DepContext(
            deps=RUN_DEPS - QUEUE_DEPS,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state)
        runnable = self.are_dependencies_met(
            dep_context=dep_context,
            session=session,
            verbose=True)

        if not runnable and not mark_success:
            # FIXME: we might have hit concurrency limits, which means we probably
            # have been running prematurely. This should be handled in the
            # scheduling mechanism.
            self.state = State.NONE
            msg = ("FIXME: Rescheduling due to concurrency limits reached at task "
                   "runtime. Attempt {attempt} of {total}. State set to NONE.").format(
                attempt=self.try_number + 1,
                total=self.max_tries + 1)
            self.log.warning(hr + msg + hr)

            self.queued_dttm = timezone.utcnow()
            self.log.info("Queuing into pool %s", self.pool)
            session.merge(self)
            session.commit()
            return False

        # Another worker might have started running this task instance while
        # the current worker process was blocked on refresh_from_db
        if self.state == State.RUNNING:
            msg = "Task Instance already running {}".format(self)
            self.log.warning(msg)
            session.commit()
            return False

        # print status message
        self.log.info(hr + msg + hr)
        self.try_number += 1

        if not test_mode:
            session.add(Log(State.RUNNING, self))
        self.state = State.RUNNING
        self.pid = os.getpid()
        self.end_date = None
        if not test_mode:
            session.merge(self)
        session.commit()

        # Closing all pooled connections to prevent
        # "max number of connections reached"
        settings.engine.dispose()
        if verbose:
            if mark_success:
                msg = "Marking success for {} on {}".format(self.task, self.execution_date)
                self.log.info(msg)
            else:
                msg = "Executing {} on {}".format(self.task, self.execution_date)
                self.log.info(msg)
        return True

    @provide_session
    def _run_raw_task(
            self,
            mark_success=False,
            test_mode=False,
            job_id=None,
            pool=None,
            session=None):
        """
        Immediately runs the task (without checking or changing db state
        before execution) and then sets the appropriate final state after
        completion and runs any post-execute callbacks. Meant to be called
        only after another function changes the state to running.

        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: boolean
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: boolean
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        """
        task = self.task
        self.pool = pool or task.pool
        self.test_mode = test_mode
        self.refresh_from_db(session=session)
        self.job_id = job_id
        self.hostname = socket.getfqdn()
        self.operator = task.__class__.__name__

        context = {}
        try:
            if not mark_success:
                context = self.get_template_context()

                task_copy = copy.copy(task)
                self.task = task_copy

                def signal_handler(signum, frame):
                    """Setting kill signal handler"""
                    self.log.error("Killing subprocess")
                    task_copy.on_kill()
                    raise AirflowException("Task received SIGTERM signal")
                signal.signal(signal.SIGTERM, signal_handler)

                # Don't clear Xcom until the task is certain to execute
                self.clear_xcom_data()

                self.render_templates()
                task_copy.pre_execute(context=context)

                # If a timeout is specified for the task, make it fail
                # if it goes beyond
                result = None
                if task_copy.execution_timeout:
                    try:
                        with timeout(int(
                                task_copy.execution_timeout.total_seconds())):
                            result = task_copy.execute(context=context)
                    except AirflowTaskTimeout:
                        task_copy.on_kill()
                        raise
                else:
                    result = task_copy.execute(context=context)

                # If the task returns a result, push an XCom containing it
                if result is not None:
                    self.xcom_push(key=XCOM_RETURN_KEY, value=result)

                # TODO remove deprecated behavior in Airflow 2.0
                try:
                    task_copy.post_execute(context=context, result=result)
                except TypeError as e:
                    if 'unexpected keyword argument' in str(e):
                        warnings.warn(
                            'BaseOperator.post_execute() now takes two '
                            'arguments, `context` and `result`, but "{}" only '
                            'expected one. This behavior is deprecated and '
                            'will be removed in a future version of '
                            'Airflow.'.format(self.task_id),
                            category=DeprecationWarning)
                        task_copy.post_execute(context=context)
                    else:
                        raise

                Stats.incr('operator_successes_{}'.format(
                    self.task.__class__.__name__), 1, 1)
                Stats.incr('ti_successes')
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SUCCESS
        except AirflowSkipException:
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SKIPPED
        except AirflowException as e:
            self.refresh_from_db()
            # for case when task is marked as success externally
            # current behavior doesn't hit the success callback
            if self.state == State.SUCCESS:
                return
            else:
                self.handle_failure(e, test_mode, context)
                raise
        except (Exception, KeyboardInterrupt) as e:
            self.handle_failure(e, test_mode, context)
            raise

        # Recording SUCCESS
        self.end_date = timezone.utcnow()
        self.set_duration()
        if not test_mode:
            session.add(Log(self.state, self))
            session.merge(self)
        session.commit()

        # Success callback
        try:
            if task.on_success_callback:
                task.on_success_callback(context)
        except Exception as e3:
            self.log.error("Failed when executing success callback")
            self.log.exception(e3)

        session.commit()

    @provide_session
    def run(
            self,
            verbose=True,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            mark_success=False,
            test_mode=False,
            job_id=None,
            pool=None,
            session=None):
        res = self._check_and_change_state_before_execution(
            verbose=verbose,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            mark_success=mark_success,
            test_mode=test_mode,
            job_id=job_id,
            pool=pool,
            session=session)
        if res:
            self._run_raw_task(
                mark_success=mark_success,
                test_mode=test_mode,
                job_id=job_id,
                pool=pool,
                session=session)

    def dry_run(self):
        task = self.task
        task_copy = copy.copy(task)
        self.task = task_copy

        self.render_templates()
        task_copy.dry_run()

    @provide_session
    def handle_failure(self, error, test_mode=False, context=None, session=None):
        self.log.exception(error)
        task = self.task
        self.end_date = timezone.utcnow()
        self.set_duration()
        Stats.incr('operator_failures_{}'.format(task.__class__.__name__), 1, 1)
        Stats.incr('ti_failures')
        if not test_mode:
            session.add(Log(State.FAILED, self))

        # Log failure duration
        session.add(TaskFail(task, self.execution_date, self.start_date, self.end_date))

        # Let's go deeper
        try:
            # try_number is incremented by 1 during task instance run. So the
            # current task instance try_number is the try_number for the next
            # task instance run. We only mark task instance as FAILED if the
            # next task instance try_number exceeds the max_tries.
            if task.retries and self.try_number <= self.max_tries:
                self.state = State.UP_FOR_RETRY
                self.log.info('Marking task as UP_FOR_RETRY')
                if task.email_on_retry and task.email:
                    self.email_alert(error, is_retry=True)
            else:
                self.state = State.FAILED
                if task.retries:
                    self.log.info('All retries failed; marking task as FAILED')
                else:
                    self.log.info('Marking task as FAILED.')
                if task.email_on_failure and task.email:
                    self.email_alert(error, is_retry=False)
        except Exception as e2:
            self.log.error('Failed to send email to: %s', task.email)
            self.log.exception(e2)

        # Handling callbacks pessimistically
        try:
            if self.state == State.UP_FOR_RETRY and task.on_retry_callback:
                task.on_retry_callback(context)
            if self.state == State.FAILED and task.on_failure_callback:
                task.on_failure_callback(context)
        except Exception as e3:
            self.log.error("Failed at executing callback")
            self.log.exception(e3)

        if not test_mode:
            session.merge(self)
        session.commit()
        self.log.error(str(error))

    @provide_session
    def get_template_context(self, session=None):
        task = self.task
        from airflow import macros
        tables = None
        if 'tables' in task.params:
            tables = task.params['tables']

        ds = self.execution_date.isoformat()[:10]
        ts = self.execution_date.isoformat()
        yesterday_ds = (self.execution_date - timedelta(1)).isoformat()[:10]
        tomorrow_ds = (self.execution_date + timedelta(1)).isoformat()[:10]

        prev_execution_date = task.dag.previous_schedule(self.execution_date)
        next_execution_date = task.dag.following_schedule(self.execution_date)

        ds_nodash = ds.replace('-', '')
        ts_nodash = ts.replace('-', '').replace(':', '')
        yesterday_ds_nodash = yesterday_ds.replace('-', '')
        tomorrow_ds_nodash = tomorrow_ds.replace('-', '')

        ti_key_str = "{task.dag_id}__{task.task_id}__{ds_nodash}"
        ti_key_str = ti_key_str.format(**locals())

        params = {}
        run_id = ''
        dag_run = None
        if hasattr(task, 'dag'):
            if task.dag.params:
                params.update(task.dag.params)
            dag_run = (
                session.query(DagRun)
                .filter_by(
                    dag_id=task.dag.dag_id,
                    execution_date=self.execution_date)
                .first()
            )
            run_id = dag_run.run_id if dag_run else None
            session.expunge_all()
            session.commit()

        if task.params:
            params.update(task.params)

        class VariableAccessor:
            """
            Wrapper around Variable. This way you can get variables in templates by using
            {var.variable_name}.
            """

            def __init__(self):
                self.var = None

            def __getattr__(self, item):
                self.var = Variable.get(item)
                return self.var

            def __repr__(self):
                return str(self.var)

        class VariableJsonAccessor:
            def __init__(self):
                self.var = None

            def __getattr__(self, item):
                self.var = Variable.get(item, deserialize_json=True)
                return self.var

            def __repr__(self):
                return str(self.var)

        return {
            'dag': task.dag,
            'ds': ds,
            'ds_nodash': ds_nodash,
            'ts': ts,
            'ts_nodash': ts_nodash,
            'yesterday_ds': yesterday_ds,
            'yesterday_ds_nodash': yesterday_ds_nodash,
            'tomorrow_ds': tomorrow_ds,
            'tomorrow_ds_nodash': tomorrow_ds_nodash,
            'END_DATE': ds,
            'end_date': ds,
            'dag_run': dag_run,
            'run_id': run_id,
            'execution_date': self.execution_date,
            'prev_execution_date': prev_execution_date,
            'next_execution_date': next_execution_date,
            'latest_date': ds,
            'macros': macros,
            'params': params,
            'tables': tables,
            'task': task,
            'task_instance': self,
            'ti': self,
            'task_instance_key_str': ti_key_str,
            'conf': configuration,
            'test_mode': self.test_mode,
            'var': {
                'value': VariableAccessor(),
                'json': VariableJsonAccessor()
            }
        }

    def render_templates(self):
        task = self.task
        jinja_context = self.get_template_context()
        if hasattr(self, 'task') and hasattr(self.task, 'dag'):
            if self.task.dag.user_defined_macros:
                jinja_context.update(
                    self.task.dag.user_defined_macros)

        rt = self.task.render_template  # shortcut to method
        for attr in task.__class__.template_fields:
            content = getattr(task, attr)
            if content:
                rendered_content = rt(attr, content, jinja_context)
                setattr(task, attr, rendered_content)

    def email_alert(self, exception, is_retry=False):
        task = self.task
        title = "Airflow alert: {self}".format(**locals())
        exception = str(exception).replace('\n', '<br>')
        # For reporting purposes, we report based on 1-indexed,
        # not 0-indexed lists (i.e. Try 1 instead of
        # Try 0 for the first attempt).
        body = (
            "Try {try_number} out of {max_tries}<br>"
            "Exception:<br>{exception}<br>"
            "Log: <a href='{self.log_url}'>Link</a><br>"
            "Host: {self.hostname}<br>"
            "Log file: {self.log_filepath}<br>"
            "Mark success: <a href='{self.mark_success_url}'>Link</a><br>"
        ).format(try_number=self.try_number + 1, max_tries=self.max_tries + 1, **locals())
        send_email(task.email, title, body)

    def set_duration(self):
        if self.end_date and self.start_date:
            self.duration = (self.end_date - self.start_date).total_seconds()
        else:
            self.duration = None

    def xcom_push(
            self,
            key,
            value,
            execution_date=None):
        """
        Make an XCom available for tasks to pull.

        :param key: A key for the XCom
        :type key: string
        :param value: A value for the XCom. The value is pickled and stored
            in the database.
        :type value: any pickleable object
        :param execution_date: if provided, the XCom will not be visible until
            this date. This can be used, for example, to send a message to a
            task on a future date without it being immediately visible.
        :type execution_date: datetime
        """

        if execution_date and execution_date < self.execution_date:
            raise ValueError(
                'execution_date can not be in the past (current '
                'execution_date is {}; received {})'.format(
                    self.execution_date, execution_date))

        XCom.set(
            key=key,
            value=value,
            task_id=self.task_id,
            dag_id=self.dag_id,
            execution_date=execution_date or self.execution_date)

    def xcom_pull(
            self,
            task_ids,
            dag_id=None,
            key=XCOM_RETURN_KEY,
            include_prior_dates=False):
        """
        Pull XComs that optionally meet certain criteria.

        The default value for `key` limits the search to XComs
        that were returned by other tasks (as opposed to those that were pushed
        manually). To remove this filter, pass key=None (or any desired value).

        If a single task_id string is provided, the result is the value of the
        most recent matching XCom from that task_id. If multiple task_ids are
        provided, a tuple of matching values is returned. None is returned
        whenever no matches are found.

        :param key: A key for the XCom. If provided, only XComs with matching
            keys will be returned. The default key is 'return_value', also
            available as a constant XCOM_RETURN_KEY. This key is automatically
            given to XComs returned by tasks (as opposed to being pushed
            manually). To remove the filter, pass key=None.
        :type key: string
        :param task_ids: Only XComs from tasks with matching ids will be
            pulled. Can pass None to remove the filter.
        :type task_ids: string or iterable of strings (representing task_ids)
        :param dag_id: If provided, only pulls XComs from this DAG.
            If None (default), the DAG of the calling task is used.
        :type dag_id: string
        :param include_prior_dates: If False, only XComs from the current
            execution_date are returned. If True, XComs from previous dates
            are returned as well.
        :type include_prior_dates: bool
        """

        if dag_id is None:
            dag_id = self.dag_id

        pull_fn = functools.partial(
            XCom.get_one,
            execution_date=self.execution_date,
            key=key,
            dag_id=dag_id,
            include_prior_dates=include_prior_dates)

        if is_container(task_ids):
            return tuple(pull_fn(task_id=t) for t in task_ids)
        else:
            return pull_fn(task_id=task_ids)

    @provide_session
    def get_num_running_task_instances(self, session):
        TI = TaskInstance
        return session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.state == State.RUNNING
        ).count()

    def init_run_context(self):
        """
        Sets the log context.
        """
        self._set_context(self)


@functools.total_ordering
class BaseOperator(LoggingMixin):
    template_fields = []
    template_ext = []
    ui_color = '#fff'
    ui_fgcolor = '#000'

    @apply_defaults
    def __init__(
            self,
            task_id,
            owner=configuration.get('operators', 'DEFAULT_OWNER'),
            email=None,
            email_on_retry=True,
            email_on_failure=True,
            retries=0,
            retry_delay=timedelta(seconds=300),
            retry_exponential_backoff=False,
            max_retry_delay=None,
            start_date=None,
            end_date=None,
            schedule_interval=None,  # not hooked as of now
            depends_on_past=False,
            wait_for_downstream=False,
            dag=None,
            params=None,
            default_args=None,
            adhoc=False,
            priority_weight=1,
            queue=configuration.get('celery', 'default_queue'),
            pool=None,
            sla=None,
            execution_timeout=None,
            on_failure_callback=None,
            on_success_callback=None,
            on_retry_callback=None,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            resources=None,
            run_as_user=None,
            task_concurrency=None,
            *args,
            **kwargs):
        if args or kwargs:
            warnings.warn(
                'Invalid arguments were passed to {c}. Support for '
                'passing such arguments will be dropped in Airflow 2.0. '
                'Invalid arguments were:'
                '\n*args: {a}\n**kwargs: {k}'.format(
                    c=self.__class__.__name__, a=args, k=kwargs),
                category=PendingDeprecationWarning
            )
        validate_key(task_id)
        self.task_id = task_id
        self.owner = owner
        self.email = email
        self.email_on_retry = email_on_retry
        self.email_on_failure = email_on_failure
        self.start_date = start_date
        if start_date and not isinstance(start_date, datetime):
            self.log.warning("start_date for %s isn't datetime.datetime", self)
        self.end_date = end_date
        if not TriggerRule.is_valid(trigger_rule):
            raise SebFlowException(
                "The trigger_rule must be one of {all_triggers},"
                "'{d}.{t}'; received '{tr}'."
                .format(all_triggers=TriggerRule.all_triggers,
                        d=dag.dag_id, t=task_id, tr=trigger_rule))

        self.trigger_rule = trigger_rule
        self.depends_on_past = depends_on_past
        self.wait_for_downstream = wait_for_downstream
        if wait_for_downstream:
            self.depends_on_past = True

        if schedule_interval:
            self.log.warning(
                "schedule_interval is used for %s, though it has "
                "been deprecated as a task parameter, you need to "
                "specify it as a DAG parameter instead",
                self
            )
        self._schedule_interval = schedule_interval
        self.retries = retries
        self.queue = queue
        self.pool = pool
        self.sla = sla
        self.execution_timeout = execution_timeout
        self.on_failure_callback = on_failure_callback
        self.on_success_callback = on_success_callback
        self.on_retry_callback = on_retry_callback
        if isinstance(retry_delay, timedelta):
            self.retry_delay = retry_delay
        else:
            self.log.debug("Retry_delay isn't timedelta object, assuming secs")
            self.retry_delay = timedelta(seconds=retry_delay)
        self.retry_exponential_backoff = retry_exponential_backoff
        self.max_retry_delay = max_retry_delay
        self.params = params or {}  # Available in templates!
        self.adhoc = adhoc
        self.priority_weight = priority_weight
        self.resources = Resources(**(resources or {}))
        self.run_as_user = run_as_user
        self.task_concurrency = task_concurrency

        # Private attributes
        self._upstream_task_ids = []
        self._downstream_task_ids = []

        if not dag and _CONTEXT_MANAGER_DAG:
            dag = _CONTEXT_MANAGER_DAG
        if dag:
            self.dag = dag

        self._log = logging.getLogger("airflow.task.operators")

        self._comps = {
            'task_id',
            'dag_id',
            'owner',
            'email',
            'email_on_retry',
            'retry_delay',
            'retry_exponential_backoff',
            'max_retry_delay',
            'start_date',
            'schedule_interval',
            'depends_on_past',
            'wait_for_downstream',
            'adhoc',
            'priority_weight',
            'sla',
            'execution_timeout',
            'on_failure_callback',
            'on_success_callback',
            'on_retry_callback',
        }

    def __eq__(self, other):
        return (
            type(self) == type(other) and
            all(self.__dict__.get(c, None) == other.__dict__.get(c, None)
                for c in self._comps))

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return self.task_id < other.task_id

    def __hash__(self):
        hash_components = [type(self)]
        for c in self._comps:
            val = getattr(self, c, None)
            try:
                hash(val)
                hash_components.append(val)
            except TypeError:
                hash_components.append(repr(val))
        return hash(tuple(hash_components))

    def __rshift__(self, other):
        """
        Implements Self >> Other == self.set_downstream(other)

        If "Other" is a DAG, the DAG is assigned to the Operator.
        """
        if isinstance(other, DAG):
            # if this dag is already assigned, do nothing
            # otherwise, do normal dag assignment
            if not (self.has_dag() and self.dag is other):
                self.dag = other
        else:
            self.set_downstream(other)
        return other

    def __lshift__(self, other):
        """
        Implements Self << Other == self.set_upstream(other)

        If "Other" is a DAG, the DAG is assigned to the Operator.
        """
        if isinstance(other, DAG):
            # if this dag is already assigned, do nothing
            # otherwise, do normal dag assignment
            if not (self.has_dag() and self.dag is other):
                self.dag = other
        else:
            self.set_upstream(other)
        return other

    def __rrshift__(self, other):
        """
        Called for [DAG] >> [Operator] because DAGs don't have
        __rshift__ operators.
        """
        self.__lshift__(other)
        return self

    def __rlshift__(self, other):
        """
        Called for [DAG] << [Operator] because DAGs don't have
        __lshift__ operators.
        """
        self.__rshift__(other)
        return self

    @property
    def dag(self):
        if self.has_dag():
            return self._dag
        else:
            raise SebFlowException('Operator {} has not been assigned to a DAG yet'.format(self))

    @dag.setter
    def dag(self, dag):
        if not isinstance(dag, DAG):
            raise TypeError('Expected DAG; recieved {}'.format(dag.__class__.__name__))
        elif self.has_dag() and self.dag is not dag:
            raise SebFlowException('The DAG assigned to {} cannot be changed.'.format(self))
        elif self.task_id not in dag.task_dict:
            dag.add_task(self)
        self._dag = dag

    def has_dag(self):
        return getattr(self, '_dag', None) is not None

    @property
    def dag_id(self):
        if self.has_dag():
            return self.dag.dag_id
        else:
            return 'no dag id'
    # @property
    # def deps(self):
    #     return {
    #         NotInRetryPeriodDep(),
    #         PrevDagrunDep(),
    #         TriggerRuleDep(),
    #     }

    @property
    def schedule_interval(self):
        if self.has_dag():
            return self.dag._schedule_interval
        else:
            return self._schedule_interval

    @property
    def priority_weight_total(self):
        return sum([
            t.priority_weight
            for t in self.get_flat_relatives(upstream=False)
        ]) + self.priority_weight

    def pre_execute(self, context):
        pass

    def execute(self, context):
        raise NotImplementedError()

    def post_execute(self, context, result=None):
        pass

    def on_kill(self):
        pass

    def __deepcopy__(self, memo):
        sys.setrecursionlimit(5000)  # TODO fix this in a better way
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result

        for k, v in list(self.__dict__.items()):
            if k not in ('user_defined_macros', 'user_defined_filters',
                         'params', '_log'):
                setattr(result, k, copy.deepcopy(v, memo))
        result.params = self.params
        if hasattr(self, 'user_defined_macros'):
            result.user_defined_macros = self.user_defined_macros
        if hasattr(self, 'user_defined_filters'):
            result.user_defined_filters = self.user_defined_filters
        if hasattr(self, '_log'):
            result._log = self._log
        return result

    def __getstate__(self):
        state = dict(self.__dict__)
        del state['_log']

        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self._log = logging.getLogger("airflow.task.operators")

    @property
    def upstream_list(self):
        return [self.dag.get_task(tid) for tid in self._upstream_task_ids]

    @property
    def upstream_task_ids(self):
        return self._upstream_task_ids

    @property
    def downstream_list(self):
        """@property: list of tasks directly downstream"""
        return [self.dag.get_task(tid) for tid in self._downstream_task_ids]

    @property
    def downstream_task_ids(self):
        return self._downstream_task_ids

    def get_task_instances(self, session, start_date=None, end_date=None):
        """
        Get a set of task instance related to this task for a specific date
        range.
        """
        TI = TaskInstance
        end_date = end_date or timezone.utcnow()
        return session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date >= start_date,
            TI.execution_date <= end_date,
        ).order_by(TI.execution_date).all()

    def get_flat_relatives(self, upstream=False, l=None):
        """
        Get a flat list of relatives, either upstream or downstream.
        """
        if not l:
            l = []
        for t in self.get_direct_relatives(upstream):
            if not is_in(t, l):
                l.append(t)
                t.get_flat_relatives(upstream, l)
        return l

    def detect_downstream_cycle(self, task=None):
        """
        When invoked, this routine will raise an exception if a cycle is
        detected downstream from self. It is invoked when tasks are added to
        the DAG to detect cycles.
        """
        if not task:
            task = self
        for t in self.get_direct_relatives():
            if task is t:
                msg = "Cycle detected in DAG. Faulty task: {0}".format(task)
                raise SebflowException(msg)
            else:
                t.detect_downstream_cycle(task=task)
        return False

    def run(
            self,
            start_date=None,
            end_date=None,
            ignore_first_depends_on_past=False,
            ignore_ti_state=False,
            mark_success=False):
        """
        Run a set of task instances for a date range.
        """
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date or timezone.utcnow()

        for dt in self.dag.date_range(start_date, end_date=end_date):
            TaskInstance(self, dt).run(
                mark_success=mark_success,
                ignore_depends_on_past=(
                    dt == start_date and ignore_first_depends_on_past),
                ignore_ti_state=ignore_ti_state)

    def get_direct_relatives(self, upstream=False):
        """
        Get the direct relatives to the current task, upstream or
        downstream.
        """
        if upstream:
            return self.upstream_list
        else:
            return self.downstream_list

    def __repr__(self):
        return "<Task(){self.__class__.__name__}): {self.task_id}>".format(self=self)

    @property
    def task_type(self):
        return self.__class__.__name__

    def append_only_new(self, l, item):
        if any([item is t for t in l]):
            raise AirflowException(
                'Dependency {self}, {item} already registered'
                ''.format(**locals()))
        else:
            l.append(item)

    def append_only_new(self, l, item):
        if any([item is t for t in l]):
            raise SebFlowException('dependency {self}, {items} already registered'.format(**locals()))
        else:
            l.append(item)

    def _set_relatives(self, task_or_task_list, upstream=False):
        try:
            task_list = list(task_or_task_list)
        except TypeError:
            task_list = [task_or_task_list]
        for t in task_list:
            if not isinstance(t, BaseOperator):
                raise SebFlowException('relationships can only be between operators; recieved {}'.format(t.__class__.__name__))

        dags = set(t.dag for t in [self] + task_list if t.has_dag())

        if len(dags) > 1:
            raise SebFlowException('tried to set relationships between tasks in more than on DAG: {}'.format(dags))
        elif len(dags) == 1:
            dag = list(dags)[0]
        else:
            raise SebFlowException('tried to create relationships between tasks that dont habe DAGs yet')

        if dag and not self.has_dag():
            self.dag = dag

        for task in task_list:
            if dag and not task.has_dag():
                task.dag = dag
            if upstream:
                task.append_only_new(task._downstream_task_ids, self.task_id)
                self.append_only_new(self._upstream_task_ids, task.task_id)
            else:
                self.append_only_new(self._downstream_task_ids, task.task_id)
                task.append_only_new(task._upstream_task_ids, self.task_id)

    def set_upstream(self, task_or_task_list):
        self._set_relatives(task_or_task_list, upstream=True)

    def set_downstream(self, task_or_task_list):
        self._set_relatives(task_or_task_list, upstream=False)


class DAG(object):
    def __init__(self,
                 dag_id,
                 description='',
                 schedule_interval=timedelta(days=1),
                 start_date=None, end_date=None,
                 full_filepath=None,
                 template_searchpath=None,
                 user_defined_macros=None,
                 user_defined_filters=None,
                 default_args=None,
                 concurrency=configuration.getint('core', 'dag_concurrency'),
                 max_active_runs=configuration.getint(
                     'core', 'max_active_runs_per_dag'),
                 dagrun_timeout=None,
                 sla_miss_callback=None,
                 default_view=configuration.get('webserver', 'dag_default_view').lower(),
                 orientation=configuration.get('webserver', 'dag_orientation'),
                 catchup=configuration.getboolean('scheduler', 'catchup_by_default'),
                 params=None
                 ):

        self.user_defined_macros = user_defined_macros
        self.user_defined_filters = user_defined_filters
        self.default_args = default_args or {}
        self.params = params or {}

        # merging potentially conflicting default_args['params'] into params
        if 'params' in self.default_args:
            self.params.update(self.default_args['params'])
            del self.default_args['params']

        validate_key(dag_id)

        # Properties from BaseDag
        self._dag_id = dag_id
        self._full_filepath = full_filepath if full_filepath else ''
        self._concurrency = concurrency
        self._pickle_id = None

        self._description = description
        # set file location to caller source path
        self.fileloc = sys._getframe().f_back.f_code.co_filename
        self.task_dict = dict()

        # set timezone
        if start_date and start_date.tzinfo:
            self.timezone = start_date.tzinfo
        elif 'start_date' in self.default_args:
            if isinstance(self.default_args['start_date'], six.string_types):
                self.default_args['start_date'] = (
                    timezone.parse(self.default_args['start_date'])
                )
            self.timezone = self.default_args['start_date'].tzinfo
        else:
            self.timezone = settings.TIMEZONE

        self.start_date = timezone.convert_to_utc(start_date)
        self.end_date = timezone.convert_to_utc(end_date)

        # also convert tasks
        if 'start_date' in self.default_args:
            self.default_args['start_date'] = (
                timezone.convert_to_utc(self.default_args['start_date'])
            )
        if 'end_date' in self.default_args:
            self.default_args['end_date'] = (
                timezone.convert_to_utc(self.default_args['end_date'])
            )

        self.schedule_interval = schedule_interval
        if schedule_interval in cron_presets:
            self._schedule_interval = cron_presets.get(schedule_interval)
        elif schedule_interval == '@once':
            self._schedule_interval = None
        else:
            self._schedule_interval = schedule_interval
        if isinstance(template_searchpath, six.string_types):
            template_searchpath = [template_searchpath]
        self.template_searchpath = template_searchpath
        self.parent_dag = None  # Gets set when DAGs are loaded
        self.last_loaded = timezone.utcnow()
        self.safe_dag_id = dag_id.replace('.', '__dot__')
        self.max_active_runs = max_active_runs
        self.dagrun_timeout = dagrun_timeout
        self.sla_miss_callback = sla_miss_callback
        self.default_view = default_view
        self.orientation = orientation
        self.catchup = catchup
        self.is_subdag = False  # DagBag.bag_dag() will set this to True if appropriate

        self.partial = False

        self._comps = {
            'dag_id',
            'task_ids',
            'parent_dag',
            'start_date',
            'schedule_interval',
            'full_filepath',
            'template_searchpath',
            'last_loaded',
        }

    def __repr__(self):
        return "<DAG: {self._dag_id}>".format(self=self)

    def __eq__(self, other):
        return (
            type(self) == type(other) and
            # Use getattr() instead of __dict__ as __dict__ doesn't return
            # correct values for properties.
            all(getattr(self, c, None) == getattr(other, c, None)
                for c in self._comps))

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return self.dag_id < other.dag_id

    def __hash__(self):
        hash_components = [type(self)]
        for c in self._comps:
            # task_ids returns a list and lists can't be hashed
            if c == 'task_ids':
                val = tuple(self.task_dict.keys())
            else:
                val = getattr(self, c, None)
            try:
                hash(val)
                hash_components.append(val)
            except TypeError:
                hash_components.append(repr(val))
        return hash(tuple(hash_components))

    # Context Manager -----------------------------------------------

    def __enter__(self):
        global _CONTEXT_MANAGER_DAG
        self._old_context_manager_dag = _CONTEXT_MANAGER_DAG
        _CONTEXT_MANAGER_DAG = self
        return self

    def __exit__(self, _type, _value, _tb):
        global _CONTEXT_MANAGER_DAG
        _CONTEXT_MANAGER_DAG = self._old_context_manager_dag

    # /Context Manager ----------------------------------------------

    def date_range(self, start_date, num=None, end_date=timezone.utcnow()):
        if num:
            end_date = None
        return utils_date_range(
            start_date=start_date, end_date=end_date,
            num=num, delta=self._schedule_interval)

    def following_schedule(self, dttm):
        """
        Calculates the following schedule for this dag in local time
        :param dttm: utc datetime
        :return: utc datetime
        """
        if isinstance(self._schedule_interval, six.string_types):
            dttm = timezone.make_naive(dttm, self.timezone)
            cron = croniter(self._schedule_interval, dttm)
            following = timezone.make_aware(cron.get_next(datetime), self.timezone)
            return timezone.convert_to_utc(following)
        elif isinstance(self._schedule_interval, timedelta):
            return dttm + self._schedule_interval

    def previous_schedule(self, dttm):
        """
        Calculates the previous schedule for this dag in local time
        :param dttm: utc datetime
        :return: utc datetime
        """
        if isinstance(self._schedule_interval, six.string_types):
            dttm = timezone.make_naive(dttm, self.timezone)
            cron = croniter(self._schedule_interval, dttm)
            prev = timezone.make_aware(cron.get_prev(datetime), self.timezone)
            return timezone.convert_to_utc(prev)
        elif isinstance(self._schedule_interval, timedelta):
            return dttm - self._schedule_interval

    def get_run_dates(self, start_date, end_date=None):
        """
        Returns a list of dates between the interval received as parameter using this
        dag's schedule interval. Returned dates can be used for execution dates.

        :param start_date: the start date of the interval
        :type start_date: datetime
        :param end_date: the end date of the interval, defaults to timezone.utcnow()
        :type end_date: datetime
        :return: a list of dates within the interval following the dag's schedule
        :rtype: list
        """
        run_dates = []

        using_start_date = start_date
        using_end_date = end_date

        # dates for dag runs
        using_start_date = using_start_date or min([t.start_date for t in self.tasks])
        using_end_date = using_end_date or timezone.utcnow()

        # next run date for a subdag isn't relevant (schedule_interval for subdags
        # is ignored) so we use the dag run's start date in the case of a subdag
        next_run_date = (self.normalize_schedule(using_start_date)
                         if not self.is_subdag else using_start_date)

        while next_run_date and next_run_date <= using_end_date:
            run_dates.append(next_run_date)
            next_run_date = self.following_schedule(next_run_date)

        return run_dates

    def normalize_schedule(self, dttm):
        """
        Returns dttm + interval unless dttm is first interval then it returns dttm
        """
        following = self.following_schedule(dttm)

        # in case of @once
        if not following:
            return dttm
        if self.previous_schedule(following) != dttm:
            return following

        return dttm

    @provide_session
    def get_last_dagrun(self, session=None, include_externally_triggered=False):
        """
        Returns the last dag run for this dag, None if there was none.
        Last dag run can be any type of run eg. scheduled or backfilled.
        Overridden DagRuns are ignored
        """
        DR = DagRun
        qry = session.query(DR).filter(
            DR.dag_id == self.dag_id,
        )
        if not include_externally_triggered:
            qry = qry.filter(DR.external_trigger.__eq__(False))

        qry = qry.order_by(DR.execution_date.desc())

        last = qry.first()

        return last

    @property
    def dag_id(self):
        return self._dag_id

    @dag_id.setter
    def dag_id(self, val):
        self._dag_id = value

    @property
    def description(self):
        return self._description

    @property
    def tasks(self):
        return list(self.task_dict.values())

    @tasks.setter
    def tasks(self, val):
        raise AttributeError('DAG.tasks cannot be modified. Use dag.add_task() instead.')

    @property
    def task_ids(self):
        return list(self.task_dict.keys())

    def has_task(self, task_id):
        return task_id in (t.task_id for t in self.tasks)

    def get_task(self, task_id):
        if task_id in self.task_dict:
            return self.task_dict[task_id]
        raise SebFlowException('Task {task_id} not found'.format(**locals()))

    def add_task(self, task):
        if task.task_id in self.task_dict:
            raise SebFlowException('task {} already exists in dag'.format(str(task)))
        else:
            self.tasks.append(task)
            self.task_dict[task.task_id] = task
            task.dag = self
        self.task_count = len(self.tasks)

    def add_tasks(self, tasks):
        for task in tasks:
            self.add_task(task)

    def run(  # 3732
            self,
            start_date=timezone.utcnow(),
            end_date=None,
            mark_success=False,
            local=False,
            executor=None,
            pool=None):
        from sebflow.jobs import BackfillJob
        job = BackfillJob(
            self,
            start_date=start_date,
            end_date=end_date,
            mark_success=mark_success,
            executor=LocalExecutor(),
            pool=pool)
        job.run()


class XCom(Base, LoggingMixin):
    """
    Base class for XCom objects.
    """
    __tablename__ = "xcom"

    id = Column(Integer, primary_key=True)
    key = Column(String(512))
    value = Column(LargeBinary)
    timestamp = Column(
        DateTime, default=func.now(), nullable=False)
    execution_date = Column(UtcDateTime, nullable=False)

    # source information
    task_id = Column(String(ID_LEN), nullable=False)
    dag_id = Column(String(ID_LEN), nullable=False)

    __table_args__ = (
        Index('idx_xcom_dag_task_date', dag_id, task_id, execution_date, unique=False),
    )

    def __repr__(self):
        return '<XCom "{key}" ({task_id} @ {execution_date})>'.format(
            key=self.key,
            task_id=self.task_id,
            execution_date=self.execution_date)

    @classmethod
    @provide_session
    def set(
            cls,
            key,
            value,
            execution_date,
            task_id,
            dag_id,
            enable_pickling=None,
            session=None):
        """
        Store an XCom value.
        TODO: "pickling" has been deprecated and JSON is preferred. "pickling" will be
        removed in Airflow 2.0. :param enable_pickling: If pickling is not enabled, the
        XCOM value will be parsed as JSON instead.

        :return: None
        """
        session.expunge_all()

        if enable_pickling is None:
            enable_pickling = configuration.getboolean('core', 'enable_xcom_pickling')

        if enable_pickling:
            value = pickle.dumps(value)
        else:
            try:
                value = json.dumps(value).encode('UTF-8')
            except ValueError:
                log = LoggingMixin().log
                log.error("Could not serialize the XCOM value into JSON. "
                          "If you are using pickles instead of JSON "
                          "for XCOM, then you need to enable pickle "
                          "support for XCOM in your airflow config.")
                raise

        # remove any duplicate XComs
        session.query(cls).filter(
            cls.key == key,
            cls.execution_date == execution_date,
            cls.task_id == task_id,
            cls.dag_id == dag_id).delete()

        session.commit()

        # insert new XCom
        session.add(XCom(
            key=key,
            value=value,
            execution_date=execution_date,
            task_id=task_id,
            dag_id=dag_id))

        session.commit()

    @classmethod
    @provide_session
    def get_one(
            cls,
            execution_date,
            key=None,
            task_id=None,
            dag_id=None,
            include_prior_dates=False,
            enable_pickling=None,
            session=None):
        """
        Retrieve an XCom value, optionally meeting certain criteria.
        TODO: "pickling" has been deprecated and JSON is preferred. "pickling" will be removed in Airflow 2.0.

        :param enable_pickling: If pickling is not enabled, the XCOM value will be parsed to JSON instead.
        :return: XCom value
        """
        filters = []
        if key:
            filters.append(cls.key == key)
        if task_id:
            filters.append(cls.task_id == task_id)
        if dag_id:
            filters.append(cls.dag_id == dag_id)
        if include_prior_dates:
            filters.append(cls.execution_date <= execution_date)
        else:
            filters.append(cls.execution_date == execution_date)

        query = (
            session.query(cls.value)
            .filter(and_(*filters))
            .order_by(cls.execution_date.desc(), cls.timestamp.desc()))

        result = query.first()
        if result:
            if enable_pickling is None:
                enable_pickling = configuration.getboolean('core', 'enable_xcom_pickling')

            if enable_pickling:
                return pickle.loads(result.value)
            else:
                try:
                    return json.loads(result.value.decode('UTF-8'))
                except ValueError:
                    log = LoggingMixin().log
                    log.error("Could not serialize the XCOM value into JSON. "
                              "If you are using pickles instead of JSON "
                              "for XCOM, then you need to enable pickle "
                              "support for XCOM in your airflow config.")
                    raise

    @classmethod
    @provide_session
    def get_many(
            cls,
            execution_date,
            key=None,
            task_ids=None,
            dag_ids=None,
            include_prior_dates=False,
            limit=100,
            enable_pickling=None,
            session=None):
        """
        Retrieve an XCom value, optionally meeting certain criteria
        TODO: "pickling" has been deprecated and JSON is preferred. "pickling" will be removed in Airflow 2.0.
        """
        filters = []
        if key:
            filters.append(cls.key == key)
        if task_ids:
            filters.append(cls.task_id.in_(as_tuple(task_ids)))
        if dag_ids:
            filters.append(cls.dag_id.in_(as_tuple(dag_ids)))
        if include_prior_dates:
            filters.append(cls.execution_date <= execution_date)
        else:
            filters.append(cls.execution_date == execution_date)

        query = (
            session.query(cls)
            .filter(and_(*filters))
            .order_by(cls.execution_date.desc(), cls.timestamp.desc())
            .limit(limit))
        results = query.all()
        if enable_pickling is None:
            enable_pickling = configuration.getboolean('core', 'enable_xcom_pickling')
        for result in results:
            if enable_pickling:
                result.value = pickle.loads(result.value)
            else:
                try:
                    result.value = json.loads(result.value.decode('UTF-8'))
                except ValueError:
                    log = LoggingMixin().log
                    log.error("Could not serialize the XCOM value into JSON. "
                              "If you are using pickles instead of JSON "
                              "for XCOM, then you need to enable pickle "
                              "support for XCOM in your airflow config.")
                    raise
        return results

    @classmethod
    @provide_session
    def delete(cls, xcoms, session=None):
        if isinstance(xcoms, XCom):
            xcoms = [xcoms]
        for xcom in xcoms:
            if not isinstance(xcom, XCom):
                raise TypeError(
                    'Expected XCom; received {}'.format(xcom.__class__.__name__)
                )
            session.delete(xcom)
        session.commit()
