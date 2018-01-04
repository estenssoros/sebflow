import datetime as dt
import hashlib
import imp
import sys
import time
import warnings

from sebflow import settings
from sebflow.utils.dates import cron_presets
from sebflow.utils.decorators import apply_defaults
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class BaseOperator(object):
    @apply_defaults
    def __init__(self,
                 task_id,
                 email=None,
                 email_on_retry=True,
                 email_on_failure=True,
                 retries=0,
                 retry_delay=dt.timedelta(seconds=300),
                 dag=None,
                 params=None,
                 default_args=None):

        self.task_id = task_id
        self.email = email
        self.email_on_retry = email_on_retry
        self.email_on_failure = email_on_failure
        self.dag = dag
        self.retries = retries

        assert(isinstance(retry_delay, dt.timedelta))
        self.retry_delay = retry_delay

        self.upstream_tasks = []
        self.downstream_tasks = []
        self.status = ''

        self._upstream_task_ids = []
        self._downstream_task_ids = []

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

    @property
    def upstream_list(self):
        return[self.dag.get_task(tid) for tid in self._upstream_task_ids]

    @property
    def upstream_task_ids(self):
        return self._upstream_task_ids

    @property
    def downstream_list(self):
        return [self.dag.get_task(tid) for tid in self._downstream_task_ids]

    @property
    def downstream_task_ids(self):
        return self._downstream_task_ids

    def __repr__(self):
        return "<Task(){self.__class__.__name__}): {self.task_id}>".format(self=self)

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
                 schedule_interval=dt.timedelta(days=1),
                 full_filepath=None,
                 default_args=None,  # TODO: build out default args
                 params=None
                 ):

        self.default_args = default_args or {}
        self.params = params or {}

        self._dag_id = dag_id
        self._full_filepath = full_filepath if full_filepath else ''
        self._description = description

        self.schedule_interval = schedule_interval
        if schedule_interval in cron_presets:
            self._schedule_interval = cron_presets.get(schedule_interval)
        elif schedule_interval == '@once':
            self._schedule_interval = None
        else:
            self._schedule_interval = schedule_interval

        self.task_dict = dict()
        # self.executor = LocalExecutor()

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

    def run(
            self,
            start_date=None,
            end_date=None,
            mark_success=False):
        if start_date is None:
            start_date= dt.datetime.now()
        from sebflow.jobs import SebJob
        job = SebJob(
            self,
            start_date=start_date,
            end_date=end_date,
            mark_success=mark_success)
        job.run()

    def __repr__(self):
        return "<DAG: {self._dag_id}>".format(self=self)

    def has_task(self, task_id):
        return task_id in (t.task_id for t in self.tasks)

    def get_task(self, task_id):
        if task_id in self.task_dict:
            return self.task_dict[task_id]
        raise SebFlowException('Task {task_id} not found'.format(**locals()))


class DagBag(object):
    def __init__(self, dag_folder=None, executor=None):
        if executor is None:
            executor = GetDefaultExecutor()
        dag_folder = dag_folder or settings.DAGS_FOLDER
        print "filling up the DagBag from %s" % dag_folder
        self.dag_folder = dag_folder
        self.dags = {}

        self.file_last_changed = {}
        self.executor = executor
        self.collect_dags(dag_folder)

    def process_file(self, file_path):
        found_dags = []
        if not os.path.isfile(filepath):
            return found_dags

        mods = []
        with open(filepath, 'rb') as f:
            contenct = f.read()
            if not all([s in contenct for s in (b'DAG', b'sebflow')]):
                return found_dags
        print('importing %s' % filepath)

        org_mod_name, _ = os.path.splitext(os.path.split(filepath)[-1])
        mod_name = ('unusual_prefix_' + hashlib.sha1(filepath.encode('utf-8')).hexdigest() + '_' + org_mod_name)
        if mod_name in sys.modules:
            del sys.modules[mod_name]
        try:
            m = imp.load_source(mod_name, filepath)
            mods.append(m)
        except Exception as e:
            print('failed to import %s' % filepath)

        for m in mods:
            for dag in list(m.__dict__.values()):
                if isinstance(dag, DAG):
                    if not dag.full_filepath:
                        dag.full_filepath = filepath
                        dself.dag_bag(dag, parent_dag=dag, root_dat=dag)
                        found_dags.append(dag)
        return found_dags

    def collect_dags(self, dag_folder=None):
        dag_folder = dag_folder or self.dag_folder
        stats = []
        FileLoadStat = namedtuple('FileLoadStat', 'file duration dag_num task_num dags')
        for root, dirs, files in os.path.walk(dag_folder, followlinks=True):
            for f in files:
                try:
                    file_path = os.path.join(root, f)
                    if not os.path.isfile(file_path):
                        continue
                    mod_name, file_ext = os.path.splitext(os.path.split(filepath)[-1])
                    if file_ext != '.py':
                        continue
                    ts = time.time()
                    found_dags = self.process_file(file_path)
                    td = time.time() - ts

                    stats.append(FileLoadStat(
                        filepath.replace(dag_folder, ''),
                        td,
                        len(found_dags),
                        sum([len(dag.tasks for dag in found_dags)]),
                        str(dag.dag_id for dag in found_dags)
                    ))
                except Exception as e:
                    print e
                self.dagbag_stats = sorted(stats, key=lambda x: x.duration, reverse=True)

        def dagbag_report(self):
            pass
