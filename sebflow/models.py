import json
from urllib.parse import urlparse

from colorama import init
from sebflow import configuration, timezone
# from sebflow.utils.decorators import apply_defaults
from sebflow.exceptions import SebflowException
from sebflow.state import State
from sebflow.utils.db import get_or_create, provide_session
from sebflow.utils.log.logging_mixin import LoggingMixin
from sqlalchemy import Boolean, Column, Integer, String, func
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import synonym
from sqlalchemy_utc import UtcDateTime
from termcolor import colored

init()

Base = declarative_base()


def get_fernet():
    """
    Deferred load of Fernet key.

    This function could fail either because Cryptography is not installed
    or because the Fernet key is invalid.

    :return: Fernet object
    :raises: AirflowException if there's a problem trying to load Fernet
    """
    try:
        from cryptography.fernet import Fernet
    except:
        raise SebflowException('Failed to import Fernet, it may not be installed')
    try:
        return Fernet(configuration.get('core', 'FERNET_KEY').encode('utf-8'))
    except ValueError as ve:
        raise SebflowException("Could not create Fernet object: {}".format(ve))


class DagModel(Base):
    __tablename__ = 'dag'
    dag_id = Column(String(50), primary_key=True)
    is_paused = Column(Boolean, unique=False, default=False)
    schedule_interval = Column(String(10))
    last_run_date = Column(UtcDateTime,)
    last_run_result = Column(String(50))

    def __repr__(self):
        return "<DAG: {self.dag_id}>".format(self=self)

    @classmethod
    @provide_session
    def get_current(cls, dag_id, session=None):
        return session.query(cls).filter(cls.dag_id == dag_id).first()


class DAG(LoggingMixin):

    def __init__(self,
                 dag_id,
                 schedule_interval='@daily',
                 ):
        self._dag_id = dag_id
        self.dag_run_id = None
        self.schedule_interval = schedule_interval
        self.task_dict = dict()

    def __repr__(self):
        return "<DAG: {self._dag_id}>".format(self=self)

    @property
    def dag_id(self):
        return self._dag_id

    @dag_id.setter
    def dag_id(self, val):
        self._dag_id = val

    @property
    def tasks(self):
        return list(self.task_dict.values())

    @property
    def task_ids(self):
        return list(self.task_dict.keys())

    def topological_sort(self):
        """
        Sorts tasks in topographical order, such that a task comes after any of its
        upstream dependencies.

        Heavily inspired by:
        http://blog.jupo.org/2012/04/06/topological-sorting-acyclic-directed-graphs/

        :return: list of tasks in topological order
        """

        # copy the the tasks so we leave it unmodified
        graph_unsorted = self.tasks[:]

        graph_sorted = []

        # special case
        if len(self.tasks) == 0:
            return tuple(graph_sorted)

        # Run until the unsorted graph is empty.
        while graph_unsorted:
            # Go through each of the node/edges pairs in the unsorted
            # graph. If a set of edges doesn't contain any nodes that
            # haven't been resolved, that is, that are still in the
            # unsorted graph, remove the pair from the unsorted graph,
            # and append it to the sorted graph. Note here that by using
            # using the items() method for iterating, a copy of the
            # unsorted graph is used, allowing us to modify the unsorted
            # graph as we move through it. We also keep a flag for
            # checking that that graph is acyclic, which is true if any
            # nodes are resolved during each pass through the graph. If
            # not, we need to bail out as the graph therefore can't be
            # sorted.
            acyclic = False
            for node in list(graph_unsorted):
                for edge in node.upstream_list:
                    if edge in graph_unsorted:
                        break
                # no edges in upstream tasks
                else:
                    acyclic = True
                    graph_unsorted.remove(node)
                    graph_sorted.append(node)

            if not acyclic:
                raise SebflowException("A cyclic dependency occurred in dag: {}".format(self.dag_id))

        return tuple(graph_sorted)

    def has_task(self, task_id):
        return task_id in (t.task_id for t in self.tasks)

    def get_task(self, task_id):
        if task_id in self.task_dict:
            return self.task_dict[task_id]
        raise SebflowException('Task {task_id} not found'.format(**locals()))

    @property
    def roots(self):
        '''
        roots are the first tasks that need to be run
        '''
        return [t for t in self.tasks if not t.upstream_list]

    def tree_view(self):
        """
        Shows an ascii tree representation of the DAG
        """
        print '-' * 50
        print self, 'TREE VIEW'
        print '-' * 50

        def get_upstream(task, level=0):
            print((" " * level * 4) + str(task))
            level += 1
            for t in task.downstream_list:
                get_upstream(t, level)

        for t in self.roots:
            get_upstream(t)
        print '-' * 50

    def add_task(self, task):
        if task.task_id in self.task_dict:
            raise SebflowException('task {} already exists in dag'.format(str(task)))
        else:
            self.tasks.append(task)
            self.task_dict[task.task_id] = task
            task.dag = self
        self.task_count = len(self.tasks)

    def add_tasks(self, tasks):
        for task in tasks:
            self.add_task(task)

    @provide_session
    def run(self, session=None):
        # get or create DAG
        dag = get_or_create(DagModel, session=session, dag_id=self._dag_id)
        dag.schedule_interval = self.schedule_interval
        dag.last_run_date = timezone.utcnow()
        session.commit()

        self.logger.info('DAG %s [starting]' % self._dag_id)

        from sebflow.executors.local_executor import LocalExecutor

        dag_run = DagRun(dag_id=self._dag_id)
        session.add(dag_run)
        session.commit()

        self.dag_run_id = dag_run.dag_run_id

        executor = LocalExecutor(dag=self)
        executor.start()
        executor.end()

        self.logger.info('DAG %s [complete]' % self._dag_id)
        session.commit()


class BaseOperator(LoggingMixin):
    # @apply_defaults
    def __init__(self, task_id, dag=None):
        self.task_id = task_id
        self.dag = dag
        self.state = State.PENDING
        self.msg = None

        self._upstream_task_ids = []
        self._downstream_task_ids = []

    def pre_execute(self):
        pass

    def execute(self):
        raise NotImplementedError()

    def post_execute(self):
        pass

    @property
    def dag(self):
        if self.has_dag():
            return self._dag
        else:
            raise SebflowException('Operator {} has not been assigned to a DAG yet'.format(self))

    @dag.setter
    def dag(self, dag):
        if not isinstance(dag, DAG):
            raise TypeError('Expected DAG; recieved {}'.format(dag.__class__.__name__))
        elif self.has_dag() and self.dag is not dag:
            raise SebflowException('The DAG assigned to {} cannot be changed.'.format(self))
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
        return[self.dag.get_task(tid) for tid in self._downstream_task_ids]

    @property
    def downstream_task_ids(self):
        return self._downstream_task_ids

    def __repr__(self):
        state = colored(self.state, State.color(self.state))
        if self.state == State.FAILED:
            return "<Task({0}): {1}> [{2}] - {3}".format(self.__class__.__name__, self.task_id, state, self.msg)
        else:
            return "<Task({0}): {1}> [{2}]".format(self.__class__.__name__, self.task_id, state)

    def append_only_new(self, l, item):
        if any([item is t for t in l]):
            raise SebflowException('dependency {self}, {items} already registered'.format(**locals()))
        else:
            l.append(item)

    def _set_relatives(self, task_or_task_list, upstream=False):
        try:
            task_list = list(task_or_task_list)
        except TypeError:
            task_list = [task_or_task_list]
        for t in task_list:
            if not isinstance(t, BaseOperator):
                raise SebflowException('relationships can only be between operators; recieved {}'.format(t.__class__.__name__))

        dags = set(t.dag for t in [self] + task_list if t.has_dag())

        if len(dags) > 1:
            raise SebflowException('tried to set relationships between tasks in more than on DAG: {}'.format(dags))
        elif len(dags) == 1:
            dag = list(dags)[0]
        else:
            raise SebflowException('tried to create relationships between tasks that dont habe DAGs yet')

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


class DagRun(Base):
    __tablename__ = 'dag_run'
    dag_run_id = Column(Integer, primary_key=True)
    dag_id = Column(String(50))
    start_date = Column(UtcDateTime, default=func.now())
    end_date = Column(UtcDateTime)
    state = Column(String(50), default=State.RUNNING)


class Task(Base):
    __tablename__ = 'task'
    id = Column(Integer, primary_key=True)
    task_id = Column(String(50))
    dag_run_id = Column(Integer)
    state = Column(String(50), default=State.PENDING)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    hostname = Column(String(50))
    unixname = Column(String(50))


class Connection(Base, LoggingMixin):
    __tablename__ = "connection"
    id = Column(Integer(), primary_key=True)
    conn_id = Column(String(50))
    conn_type = Column(String(100))
    host = Column(String(100))
    schema = Column(String(100))
    login = Column(String(100))
    _password = Column('password', String(100))
    port = Column(Integer())
    is_encrypted = Column(Boolean, unique=False, default=False)
    is_extra_encrypted = Column(Boolean, unique=False, default=False)
    _extra = Column('extra', String(100))

    _types = [
        ('ftp', 'FTP',),
        ('postgres', 'Postgres',),
        ('oracle', 'Oracle',),
        ('s3', 'S3',),
        ('sqlite', 'Sqlite',),
        ('mssql', 'Microsoft SQL Server'),
    ]

    def __init__(
            self, conn_id=None, conn_type=None,
            host=None, login=None, password=None,
            schema=None, port=None, extra=None,
            uri=None):
        self.conn_id = conn_id
        if uri:
            self.parse_from_uri(uri)
        else:
            self.conn_type = conn_type
            self.host = host
            self.login = login
            self.password = password
            self.schema = schema
            self.port = port
            self.extra = extra

    def parse_from_uri(self, uri):
        temp_uri = urlparse(uri)
        hostname = temp_uri.hostname or ''
        if '%2f' in hostname:
            hostname = hostname.replace('%2f', '/').replace('%2F', '/')
        conn_type = temp_uri.scheme
        if conn_type == 'postgresql':
            conn_type = 'postgres'
        self.conn_type = conn_type
        self.host = hostname
        self.schema = temp_uri.path[1:]
        self.login = temp_uri.username
        self.password = temp_uri.password
        self.port = temp_uri.port

    def get_password(self):
        if self._password and self.is_encrypted:
            try:
                fernet = get_fernet()
            except:
                raise SebflowException(
                    "Can't decrypt encrypted password for login={}, \
                    FERNET_KEY configuration is missing".format(self.login))
            return fernet.decrypt(bytes(self._password, 'utf-8')).decode()
        else:
            return self._password

    def set_password(self, value):
        if value:
            try:
                fernet = get_fernet()
                self._password = fernet.encrypt(bytes(value, 'utf-8')).decode()
                self.is_encrypted = True
            except SebflowException:
                self.log.exception("Failed to load fernet while encrypting value, "
                                   "using non-encrypted value.")
                self._password = value
                self.is_encrypted = False

    @declared_attr
    def password(cls):
        return synonym('_password',
                       descriptor=property(cls.get_password, cls.set_password))

    def get_extra(self):
        if self._extra and self.is_extra_encrypted:
            try:
                fernet = get_fernet()
            except:
                raise SebflowException(
                    "Can't decrypt `extra` params for login={},\
                    FERNET_KEY configuration is missing".format(self.login))
            return fernet.decrypt(bytes(self._extra, 'utf-8')).decode()
        else:
            return self._extra

    def set_extra(self, value):
        if value:
            try:
                fernet = get_fernet()
                self._extra = fernet.encrypt(bytes(value, 'utf-8')).decode()
                self.is_extra_encrypted = True
            except SebflowException:
                self.log.exception("Failed to load fernet while encrypting value, "
                                   "using non-encrypted value.")
                self._extra = value
                self.is_extra_encrypted = False

    @declared_attr
    def extra(cls):
        return synonym('_extra',
                       descriptor=property(cls.get_extra, cls.set_extra))

    def get_hook(self):
        try:
            if self.conn_type == 'mysql':
                from airflow.hooks.mysql_hook import MySqlHook
                return MySqlHook(mysql_conn_id=self.conn_id)
            elif self.conn_type == 'google_cloud_platform':
                from airflow.contrib.hooks.bigquery_hook import BigQueryHook
                return BigQueryHook(bigquery_conn_id=self.conn_id)
            elif self.conn_type == 'postgres':
                from airflow.hooks.postgres_hook import PostgresHook
                return PostgresHook(postgres_conn_id=self.conn_id)
            elif self.conn_type == 'hive_cli':
                from airflow.hooks.hive_hooks import HiveCliHook
                return HiveCliHook(hive_cli_conn_id=self.conn_id)
            elif self.conn_type == 'presto':
                from airflow.hooks.presto_hook import PrestoHook
                return PrestoHook(presto_conn_id=self.conn_id)
            elif self.conn_type == 'hiveserver2':
                from airflow.hooks.hive_hooks import HiveServer2Hook
                return HiveServer2Hook(hiveserver2_conn_id=self.conn_id)
            elif self.conn_type == 'sqlite':
                from airflow.hooks.sqlite_hook import SqliteHook
                return SqliteHook(sqlite_conn_id=self.conn_id)
            elif self.conn_type == 'jdbc':
                from airflow.hooks.jdbc_hook import JdbcHook
                return JdbcHook(jdbc_conn_id=self.conn_id)
            elif self.conn_type == 'mssql':
                from airflow.hooks.mssql_hook import MsSqlHook
                return MsSqlHook(mssql_conn_id=self.conn_id)
            elif self.conn_type == 'oracle':
                from airflow.hooks.oracle_hook import OracleHook
                return OracleHook(oracle_conn_id=self.conn_id)
            elif self.conn_type == 'vertica':
                from airflow.contrib.hooks.vertica_hook import VerticaHook
                return VerticaHook(vertica_conn_id=self.conn_id)
            elif self.conn_type == 'cloudant':
                from airflow.contrib.hooks.cloudant_hook import CloudantHook
                return CloudantHook(cloudant_conn_id=self.conn_id)
            elif self.conn_type == 'jira':
                from airflow.contrib.hooks.jira_hook import JiraHook
                return JiraHook(jira_conn_id=self.conn_id)
            elif self.conn_type == 'redis':
                from airflow.contrib.hooks.redis_hook import RedisHook
                return RedisHook(redis_conn_id=self.conn_id)
            elif self.conn_type == 'wasb':
                from airflow.contrib.hooks.wasb_hook import WasbHook
                return WasbHook(wasb_conn_id=self.conn_id)
            elif self.conn_type == 'docker':
                from airflow.hooks.docker_hook import DockerHook
                return DockerHook(docker_conn_id=self.conn_id)
        except:
            pass

    def __repr__(self):
        return self.conn_id

    @property
    def extra_dejson(self):
        """Returns the extra property by deserializing json."""
        obj = {}
        if self.extra:
            try:
                obj = json.loads(self.extra)
            except Exception as e:
                self.log.exception(e)
                self.log.error("Failed parsing the json for conn_id %s", self.conn_id)

        return obj
