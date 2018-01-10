import os
import random

from sebflow.execeptions import SebflowException
from sebflow.log.logging_mixin import LoggingMixin
from sebflow.models import Connection
from sebflow.utils.db import provide_session

CONN_ENV_PREFIX = 'SEBFLOW_CONN_'

class BaseHook(LoggingMixin):
    """
    Abstract base class for hooks, hooks are meant as an interface to
    interact with external systems. MySqlHook, HiveHook, PigHook return
    object that can handle the connection and interaction to specific
    instances of these systems, and expose consistent methods to interact
    with them.
    """

    def __init__(self, source):
        pass

    @classmethod
    @provide_session
    def _get_connections_from_db(cls, conn_id, session=None):
        db = (
            session.query(Connection)
            .filter(Connection.conn_id == conn_id)
            .all()
        )
        session.expunge_all()
        if not db:
            raise SebflowException(
                "The conn_id `{0}` isn't defined".format(conn_id))
        return db

    @classmethod
    def _get_connection_from_env(cls, conn_id):
        environment_uri = os.environ.get(CONN_ENV_PREFIX + conn_id.upper())
        conn = None
        if environment_uri:
            conn = Connection(conn_id=conn_id, uri=environment_uri)
        return conn

    @classmethod
    def get_connections(cls, conn_id):
        conn = cls._get_connection_from_env(conn_id)
        if conn:
            conns = [conn]
        else:
            conns = cls._get_connections_from_db(conn_id)
        return conns

    @classmethod
    def get_connection(cls, conn_id):
        conn = random.choice(cls.get_connections(conn_id))
        if conn.host:
            log = LoggingMixin().log
            log.info("Using connection to: %s", conn.host)
        return conn

    @classmethod
    def get_hook(cls, conn_id):
        connection = cls.get_connection(conn_id)
        return connection.get_hook()

    def get_conn(self):
        raise NotImplementedError()

    def get_records(self, sql):
        raise NotImplementedError()

    def get_pandas_df(self, sql):
        raise NotImplementedError()

    def run(self, sql):
        raise NotImplementedError()
