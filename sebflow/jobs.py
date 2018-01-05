import datetime as dt
import getpass
import socket

from sqlalchemy import Column, Index, Integer, String, and_, func, not_, or_
from sqlalchemy.orm.session import make_transient
from sqlalchemy_utc import UtcDateTime

from sebflow import configuration as conf
from sebflow import executors, models, settings
from sebflow.settings import Stats
from sebflow.utils import timezone
from sebflow.utils.db import create_session, provide_session
from sebflow.utils.state import State

Base = models.Base
ID_LEN = models.ID_LEN


class BaseJob(Base):
    __tablename__ = "job"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN),)
    state = Column(String(20))
    job_type = Column(String(30))
    start_date = Column(UtcDateTime())
    end_date = Column(UtcDateTime())
    latest_heartbeat = Column(UtcDateTime())
    executor_class = Column(String(500))
    hostname = Column(String(500))
    unixname = Column(String(1000))

    __mapper_args__ = {
        'polymorphic_on': job_type,
        'polymorphic_identity': 'BaseJob'
    }

    __table_args__ = (
        Index('job_type_heart', job_type, latest_heartbeat),
    )

    def __init__(self,
                 executor=executors.GetDefaultExecutor(),
                 heartrate=conf.getfloat('scheduler', 'JOB_HEARTBEAT_SEC'),
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

    def heartbeat_callback(self, session=None):
        pass

    def heartbeat(self):
        '''
        Heartbeats update the job's entry in the database with a timestamp
        for the latest_heartbeat and allows for the job to be killed
        externally. This allows at the system level to monitor what is
        actually active.

        For instance, an old heartbeat for SchedulerJob would mean something
        is wrong.

        This also allows for any job to be killed externally, regardless
        of who is running it or on which machine it is running.

        Note that if your heartbeat is set to 60 seconds and you call this
        method after 10 seconds of processing since the last heartbeat, it
        will sleep 50 seconds to complete the 60 seconds and keep a steady
        heart rate. If you go over 60 seconds before calling it, it won't
        sleep at all.
        '''
        with create_session() as session:
            job = session.query(BaseJob).filter_by(id=self.id).one()
            make_transient(job)
            session.commit()

        if job.state == State.SHUTDOWN:
            self.kill()

        # Figure out how long to sleep for
        sleep_for = 0
        if job.latest_heartbeat:
            sleep_for = max(
                0,
                self.heartrate - (timezone.utcnow() - job.latest_heartbeat).total_seconds())

        sleep(sleep_for)

        # Update last heartbeat time
        with create_session() as session:
            job = session.query(BaseJob).filter(BaseJob.id == self.id).first()
            job.latest_heartbeat = timezone.utcnow()
            session.merge(job)
            session.commit()

            self.heartbeat_callback(session=session)
            self.log.debug('[heartbeat]')

    def run(self):
        Stats.incr(self.__class__.__name__.lower() + '_start', 1, 1)
        # Adding an entry in the DB
        with create_session() as session:
            self.state = State.RUNNING
            session.add(self)
            session.commit()
            id_ = self.id
            make_transient(self)
            self.id = id_

            # Run
            self._execute()

            # Marking the success in the DB
            self.end_date = timezone.utcnow()
            self.state = State.SUCCESS
            session.merge(self)
            session.commit()

        Stats.incr(self.__class__.__name__.lower() + '_end', 1, 1)

    def _execute(self):
        raise NotImplementedError("this method need to be overridden")

    @provide_session
    def reset_state_for_orphaned_tasks(self, filter_by_dag_run=None, session=None):
        """
        This function checks if there are any tasks in the dagrun (or all)
        that have a scheduled state but are not known by the
        executor. If it finds those it will reset the state to None
        so they will get picked up again.
        The batch option is for performance reasons as the queries are made in
        sequence.

        :param filter_by_dag_run: the dag_run we want to process, None if all
        :type filter_by_dag_run: models.DagRun
        :return: the TIs reset (in expired SQLAlchemy state)
        :rtype: List(TaskInstance)
        """
        queued_tis = self.executor.queued_tasks
        # also consider running as the state might not have changed in the db yet
        running_tis = self.executor.running

        resettable_states = [State.SCHEDULED, State.QUEUED]
        TI = models.TaskInstance
        DR = models.DagRun
        if filter_by_dag_run is None:
            resettable_tis = (
                session
                .query(TI)
                .join(
                    DR,
                    and_(
                        TI.dag_id == DR.dag_id,
                        TI.execution_date == DR.execution_date))
                .filter(
                    DR.state == State.RUNNING,
                    DR.external_trigger == False,
                    DR.run_id.notlike(BackfillJob.ID_PREFIX + '%'),
                    TI.state.in_(resettable_states))).all()
        else:
            resettable_tis = filter_by_dag_run.get_task_instances(state=resettable_states,
                                                                  session=session)
        tis_to_reset = []
        # Can't use an update here since it doesn't support joins
        for ti in resettable_tis:
            if ti.key not in queued_tis and ti.key not in running_tis:
                tis_to_reset.append(ti)

        filter_for_tis = ([and_(TI.dag_id == ti.dag_id,
                                TI.task_id == ti.task_id,
                                TI.execution_date == ti.execution_date)
                           for ti in tis_to_reset])
        if len(tis_to_reset) == 0:
            return []
        reset_tis = (
            session
            .query(TI)
            .filter(or_(*filter_for_tis), TI.state.in_(resettable_states))
            .with_for_update()
            .all())
        for ti in reset_tis:
            ti.state = State.NONE
            session.merge(ti)
        task_instance_str = '\n\t'.join(
            ["{}".format(x) for x in reset_tis])
        session.commit()

        self.log.info(
            "Reset the following %s TaskInstances:\n\t%s",
            len(reset_tis), task_instance_str
        )
        return reset_tis


class BackfillJob(BaseJob):
    ID_PREFIX = 'backfill_'
    ID_FORMAT_PREFIX = ID_PREFIX + '{0}'

    __mapper_args__ = {
        'polymorphic_identity': 'BackfillJob'
    }

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

    def __init__(self,
                 dag,
                 start_date=None,
                 end_date=None,
                 mark_success=False,
                 include_adhoc=False,
                 donot_pickle=False,
                 ignore_first_depends_on_past=False,
                 ignore_task_deps=False,
                 pool=None,
                 delay_on_limit_secs=1.0,
                 *args, **kwargs):
        self.dag = dag
        self.dag_id = dag.dag_id
        self.bf_start_date = start_date
        self.bf_end_date = end_date
        self.mark_success = mark_success
        self.include_adhoc = include_adhoc
        self.donot_pickle = donot_pickle
        self.ignore_first_depends_on_past = ignore_first_depends_on_past
        self.ignore_task_deps = ignore_task_deps
        self.pool = pool
        self.delay_on_limit_secs = delay_on_limit_secs
        super(BackfillJob, self).__init__(*args, **kwargs)

    def _update_counters(self, ti_status):
        """
        Updates the counters per state of the tasks that were running. Can re-add
        to tasks to run in case required.
        :param ti_status: the internal status of the backfill job tasks
        :type ti_status: BackfillJob._DagRunTaskStatus
        """
        for key, ti in list(ti_status.started.items()):
            ti.refresh_from_db()
            if ti.state == State.SUCCESS:
                ti_status.succeeded.add(key)
                self.log.debug("Task instance %s succeeded. Don't rerun.", ti)
                ti_status.started.pop(key)
                continue
            elif ti.state == State.SKIPPED:
                ti_status.skipped.add(key)
                self.log.debug("Task instance %s skipped. Don't rerun.", ti)
                ti_status.started.pop(key)
                continue
            elif ti.state == State.FAILED:
                self.log.error("Task instance %s failed", ti)
                ti_status.failed.add(key)
                ti_status.started.pop(key)
                continue
            # special case: if the task needs to run again put it back
            elif ti.state == State.UP_FOR_RETRY:
                self.log.warning("Task instance %s is up for retry", ti)
                ti_status.started.pop(key)
                ti_status.to_run[key] = ti
            # special case: The state of the task can be set to NONE by the task itself
            # when it reaches concurrency limits. It could also happen when the state
            # is changed externally, e.g. by clearing tasks from the ui. We need to cover
            # for that as otherwise those tasks would fall outside of the scope of
            # the backfill suddenly.
            elif ti.state == State.NONE:
                self.log.warning(
                    "FIXME: task instance %s state was set to none externally or "
                    "reaching concurrency limits. Re-adding task to queue.",
                    ti
                )
                ti.set_state(State.SCHEDULED)
                ti_status.started.pop(key)
                ti_status.to_run[key] = ti

    def _manage_executor_state(self, started):
        """
        Checks if the executor agrees with the state of task instances
        that are running
        :param started: dict of key, task to verify
        """
        executor = self.executor

        for key, state in list(executor.get_event_buffer().items()):
            if key not in started:
                self.log.warning(
                    "%s state %s not in started=%s",
                    key, state, started.values()
                )
                continue

            ti = started[key]
            ti.refresh_from_db()

            self.log.debug("Executor state: %s task %s", state, ti)

            if state == State.FAILED or state == State.SUCCESS:
                if ti.state == State.RUNNING or ti.state == State.QUEUED:
                    msg = ("Executor reports task instance {} finished ({}) "
                           "although the task says its {}. Was the task "
                           "killed externally?".format(ti, state, ti.state))
                    self.log.error(msg)
                    ti.handle_failure(msg)

    @provide_session
    def _get_dag_run(self, run_date, session=None):
        """
        Returns a dag run for the given run date, which will be matched to an existing
        dag run if available or create a new dag run otherwise. If the max_active_runs
        limit is reached, this function will return None.
        :param run_date: the execution date for the dag run
        :type run_date: datetime
        :param session: the database session object
        :type session: Session
        :return: a DagRun in state RUNNING or None
        """
        run_id = BackfillJob.ID_FORMAT_PREFIX.format(run_date.isoformat())

        # consider max_active_runs but ignore when running subdags
        respect_dag_max_active_limit = (True
                                        if (self.dag.schedule_interval and
                                            not self.dag.is_subdag)
                                        else False)

        current_active_dag_count = self.dag.get_num_active_runs(external_trigger=False)

        # check if we are scheduling on top of a already existing dag_run
        # we could find a "scheduled" run instead of a "backfill"
        run = DagRun.find(dag_id=self.dag.dag_id,
                          execution_date=run_date,
                          session=session)

        if run is not None and len(run) > 0:
            run = run[0]
            if run.state == State.RUNNING:
                respect_dag_max_active_limit = False
        else:
            run = None

        # enforce max_active_runs limit for dag, special cases already
        # handled by respect_dag_max_active_limit
        if (respect_dag_max_active_limit and
                current_active_dag_count >= self.dag.max_active_runs):
            return None

        run = run or self.dag.create_dagrun(
            run_id=run_id,
            execution_date=run_date,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            external_trigger=False,
            session=session
        )

        # set required transient field
        run.dag = self.dag

        # explicitly mark as backfill and running
        run.state = State.RUNNING
        run.run_id = run_id
        run.verify_integrity(session=session)
        return run

    @provide_session
    def _task_instances_for_dag_run(self, dag_run, session=None):
        """
        Returns a map of task instance key to task instance object for the tasks to
        run in the given dag run.
        :param dag_run: the dag run to get the tasks from
        :type dag_run: models.DagRun
        :param session: the database session object
        :type session: Session
        """
        tasks_to_run = {}

        if dag_run is None:
            return tasks_to_run

        # check if we have orphaned tasks
        self.reset_state_for_orphaned_tasks(filter_by_dag_run=dag_run, session=session)

        # for some reason if we don't refresh the reference to run is lost
        dag_run.refresh_from_db()
        make_transient(dag_run)

        # TODO(edgarRd): AIRFLOW-1464 change to batch query to improve perf
        for ti in dag_run.get_task_instances():
            # all tasks part of the backfill are scheduled to run
            if ti.state == State.NONE:
                ti.set_state(State.SCHEDULED, session=session)
            tasks_to_run[ti.key] = ti

        return tasks_to_run

    def _log_progress(self, ti_status):
        msg = ' | '.join([
            "[backfill progress]",
            "finished run {0} of {1}",
            "tasks waiting: {2}",
            "succeeded: {3}",
            "kicked_off: {4}",
            "failed: {5}",
            "skipped: {6}",
            "deadlocked: {7}",
            "not ready: {8}"
        ]).format(
            ti_status.finished_runs,
            ti_status.total_runs,
            len(ti_status.to_run),
            len(ti_status.succeeded),
            len(ti_status.started),
            len(ti_status.failed),
            len(ti_status.skipped),
            len(ti_status.deadlocked),
            len(ti_status.not_ready))
        self.log.info(msg)

        self.log.debug(
            "Finished dag run loop iteration. Remaining tasks %s",
            ti_status.to_run.values()
        )

    @provide_session
    def _process_backfill_task_instances(self,
                                         ti_status,
                                         executor,
                                         pickle_id,
                                         start_date=None, session=None):
        """
        Process a set of task instances from a set of dag runs. Special handling is done
        to account for different task instance states that could be present when running
        them in a backfill process.
        :param ti_status: the internal status of the job
        :type ti_status: BackfillJob._DagRunTaskStatus
        :param executor: the executor to run the task instances
        :type executor: BaseExecutor
        :param pickle_id: the pickle_id if dag is pickled, None otherwise
        :type pickle_id: int
        :param start_date: the start date of the backfill job
        :type start_date: datetime
        :param session: the current session object
        :type session: Session
        :return: the list of execution_dates for the finished dag runs
        :rtype: list
        """

        executed_run_dates = []

        while ((len(ti_status.to_run) > 0 or len(ti_status.started) > 0) and
                len(ti_status.deadlocked) == 0):
            self.log.debug("*** Clearing out not_ready list ***")
            ti_status.not_ready.clear()

            # we need to execute the tasks bottom to top
            # or leaf to root, as otherwise tasks might be
            # determined deadlocked while they are actually
            # waiting for their upstream to finish
            for task in self.dag.topological_sort():
                for key, ti in list(ti_status.to_run.items()):
                    if task.task_id != ti.task_id:
                        continue

                    ti.refresh_from_db()

                    task = self.dag.get_task(ti.task_id)
                    ti.task = task

                    ignore_depends_on_past = (
                        self.ignore_first_depends_on_past and
                        ti.execution_date == (start_date or ti.start_date))
                    self.log.debug("Task instance to run %s state %s", ti, ti.state)

                    # guard against externally modified tasks instances or
                    # in case max concurrency has been reached at task runtime
                    if ti.state == State.NONE:
                        self.log.warning(
                            "FIXME: task instance {} state was set to None externally. This should not happen"
                        )
                        ti.set_state(State.SCHEDULED, session=session)

                    # The task was already marked successful or skipped by a
                    # different Job. Don't rerun it.
                    if ti.state == State.SUCCESS:
                        ti_status.succeeded.add(key)
                        self.log.debug("Task instance %s succeeded. Don't rerun.", ti)
                        ti_status.to_run.pop(key)
                        if key in ti_status.started:
                            ti_status.started.pop(key)
                        continue
                    elif ti.state == State.SKIPPED:
                        ti_status.skipped.add(key)
                        self.log.debug("Task instance %s skipped. Don't rerun.", ti)
                        ti_status.to_run.pop(key)
                        if key in ti_status.started:
                            ti_status.started.pop(key)
                        continue
                    elif ti.state == State.FAILED:
                        self.log.error("Task instance %s failed", ti)
                        ti_status.failed.add(key)
                        ti_status.to_run.pop(key)
                        if key in ti_status.started:
                            ti_status.started.pop(key)
                        continue
                    elif ti.state == State.UPSTREAM_FAILED:
                        self.log.error("Task instance %s upstream failed", ti)
                        ti_status.failed.add(key)
                        ti_status.to_run.pop(key)
                        if key in ti_status.started:
                            ti_status.started.pop(key)
                        continue

                    backfill_context = DepContext(
                        deps=RUN_DEPS,
                        ignore_depends_on_past=ignore_depends_on_past,
                        ignore_task_deps=self.ignore_task_deps,
                        flag_upstream_failed=True)

                    # Is the task runnable? -- then run it
                    # the dependency checker can change states of tis
                    if ti.are_dependencies_met(
                            dep_context=backfill_context,
                            session=session,
                            verbose=True):
                        ti.refresh_from_db(lock_for_update=True, session=session)
                        if ti.state == State.SCHEDULED or ti.state == State.UP_FOR_RETRY:
                            if executor.has_task(ti):
                                self.log.debug(
                                    "Task Instance %s already in executor waiting for queue to clear",
                                    ti
                                )
                            else:
                                self.log.debug('Sending %s to executor', ti)
                                # Skip scheduled state, we are executing immediately
                                ti.state = State.QUEUED
                                session.merge(ti)
                                executor.queue_task_instance(
                                    ti,
                                    mark_success=self.mark_success,
                                    pickle_id=pickle_id,
                                    ignore_task_deps=self.ignore_task_deps,
                                    ignore_depends_on_past=ignore_depends_on_past,
                                    pool=self.pool)
                                ti_status.started[key] = ti
                                ti_status.to_run.pop(key)
                        session.commit()
                        continue

                    if ti.state == State.UPSTREAM_FAILED:
                        self.log.error("Task instance %s upstream failed", ti)
                        ti_status.failed.add(key)
                        ti_status.to_run.pop(key)
                        if key in ti_status.started:
                            ti_status.started.pop(key)
                        continue

                    # special case
                    if ti.state == State.UP_FOR_RETRY:
                        self.log.debug("Task instance %s retry period not expired yet", ti)
                        if key in ti_status.started:
                            ti_status.started.pop(key)
                        ti_status.to_run[key] = ti
                        continue

                    # all remaining tasks
                    self.log.debug('Adding %s to not_ready', ti)
                    ti_status.not_ready.add(key)

            # execute the tasks in the queue
            self.heartbeat()
            executor.heartbeat()

            # If the set of tasks that aren't ready ever equals the set of
            # tasks to run and there are no running tasks then the backfill
            # is deadlocked
            if (ti_status.not_ready and
                    ti_status.not_ready == set(ti_status.to_run) and
                    len(ti_status.started) == 0):
                self.log.warning(
                    "Deadlock discovered for ti_status.to_run=%s",
                    ti_status.to_run.values()
                )
                ti_status.deadlocked.update(ti_status.to_run.values())
                ti_status.to_run.clear()

            # check executor state
            self._manage_executor_state(ti_status.started)

            # update the task counters
            self._update_counters(ti_status=ti_status)

            # update dag run state
            _dag_runs = ti_status.active_runs[:]
            for run in _dag_runs:
                run.update_state(session=session)
                if run.state in State.finished():
                    ti_status.finished_runs += 1
                    ti_status.active_runs.remove(run)
                    executed_run_dates.append(run.execution_date)

                if run.dag.is_paused:
                    models.DagStat.update([run.dag_id], session=session)

            self._log_progress(ti_status)

        # return updated status
        return executed_run_dates

    @provide_session
    def _collect_errors(self, ti_status, session=None):
        err = ''
        if ti_status.failed:
            err += (
                "---------------------------------------------------\n"
                "Some task instances failed:\n%s\n".format(ti_status.failed))
        if ti_status.deadlocked:
            err += (
                '---------------------------------------------------\n'
                'BackfillJob is deadlocked.')
            deadlocked_depends_on_past = any(
                t.are_dependencies_met(
                    dep_context=DepContext(ignore_depends_on_past=False),
                    session=session,
                    verbose=True) !=
                t.are_dependencies_met(
                    dep_context=DepContext(ignore_depends_on_past=True),
                    session=session,
                    verbose=True)
                for t in ti_status.deadlocked)
            if deadlocked_depends_on_past:
                err += (
                    'Some of the deadlocked tasks were unable to run because '
                    'of "depends_on_past" relationships. Try running the '
                    'backfill with the option '
                    '"ignore_first_depends_on_past=True" or passing "-I" at '
                    'the command line.')
            err += ' These tasks have succeeded:\n{}\n'.format(ti_status.succeeded)
            err += ' These tasks have started:\n{}\n'.format(ti_status.started)
            err += ' These tasks have failed:\n{}\n'.format(ti_status.failed)
            err += ' These tasks are skipped:\n{}\n'.format(ti_status.skipped)
            err += ' These tasks are deadlocked:\n{}\n'.format(ti_status.deadlocked)

        return err

    @provide_session
    def _execute_for_run_dates(self, run_dates, ti_status, executor, pickle_id,
                               start_date, session=None):
        """
        Computes the dag runs and their respective task instances for
        the given run dates and executes the task instances.
        Returns a list of execution dates of the dag runs that were executed.
        :param run_dates: Execution dates for dag runs
        :type run_dates: list
        :param ti_status: internal BackfillJob status structure to tis track progress
        :type ti_status: BackfillJob._DagRunTaskStatus
        :param executor: the executor to use, it must be previously started
        :type executor: BaseExecutor
        :param pickle_id: numeric id of the pickled dag, None if not pickled
        :type pickle_id: int
        :param start_date: backfill start date
        :type start_date: datetime
        :param session: the current session object
        :type session: Session
        """
        for next_run_date in run_dates:
            dag_run = self._get_dag_run(next_run_date, session=session)
            tis_map = self._task_instances_for_dag_run(dag_run,
                                                       session=session)
            if dag_run is None:
                continue

            ti_status.active_runs.append(dag_run)
            ti_status.to_run.update(tis_map or {})

        processed_dag_run_dates = self._process_backfill_task_instances(
            ti_status=ti_status,
            executor=executor,
            pickle_id=pickle_id,
            start_date=start_date,
            session=session)

        ti_status.executed_dag_run_dates.update(processed_dag_run_dates)

    @provide_session
    def _execute(self, session=None):
        """
        Initializes all components required to run a dag for a specified date range and
        calls helper method to execute the tasks.
        """
        ti_status = BackfillJob._DagRunTaskStatus()

        start_date = self.bf_start_date

        # Get intervals between the start/end dates, which will turn into dag runs
        run_dates = self.dag.get_run_dates(start_date=start_date,
                                           end_date=self.bf_end_date)
        if len(run_dates) == 0:
            self.log.info("No run dates were found for the given dates and dag interval.")
            return

        # picklin'
        pickle_id = None
        if not self.donot_pickle and self.executor.__class__ not in (
                executors.LocalExecutor, executors.SequentialExecutor):
            pickle = models.DagPickle(self.dag)
            session.add(pickle)
            session.commit()
            pickle_id = pickle.id

        executor = self.executor
        executor.start()

        ti_status.total_runs = len(run_dates)  # total dag runs in backfill

        try:
            remaining_dates = ti_status.total_runs
            while remaining_dates > 0:
                dates_to_process = [run_date for run_date in run_dates
                                    if run_date not in ti_status.executed_dag_run_dates]

                self._execute_for_run_dates(run_dates=dates_to_process,
                                            ti_status=ti_status,
                                            executor=executor,
                                            pickle_id=pickle_id,
                                            start_date=start_date,
                                            session=session)

                remaining_dates = (
                    ti_status.total_runs - len(ti_status.executed_dag_run_dates)
                )
                err = self._collect_errors(ti_status=ti_status, session=session)
                if err:
                    raise AirflowException(err)

                if remaining_dates > 0:
                    self.log.info(
                        "max_active_runs limit for dag %s has been reached "
                        " - waiting for other dag runs to finish",
                        self.dag_id
                    )
                    time.sleep(self.delay_on_limit_secs)
        finally:
            executor.end()
            session.commit()

        self.log.info("Backfill done. Exiting.")


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
        self.file_process_interval = file_process_interval
