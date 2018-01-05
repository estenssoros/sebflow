import logging
import os
import sys

import pendulum
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from sebflow import configuration as conf

log = logging.getLogger(__name__)
# TIMEZONE='America/Denver'
TIMEZONE = pendulum.local_timezone()
PARALLELISM = 32


class DummyStatsLogger(object):
    @classmethod
    def incr(cls, stat, count=1, rate=1):
        pass

    @classmethod
    def decr(cls, stat, count=1, rate=1):
        pass

    @classmethod
    def gauge(cls, stat, value, rate=1, delta=False):
        pass

    @classmethod
    def timing(cls, stat, dt):
        pass


Stats = DummyStatsLogger

HEADER = '''
   _____ __________  ________    ____ _       __
  / ___// ____/ __ )/ ____/ /   / __ \ |     / /
  \__ \/ __/ / __  / /_  / /   / / / / | /| / /
 ___/ / /___/ /_/ / __/ / /___/ /_/ /| |/ |/ /
/____/_____/_____/_/   /_____/\____/ |__/|__/

'''

DB_CREDS = {
    'user': 'seb',
    'password': 'sebflow132435',
    'host': '127.0.0.1',
    'dbname': 'sebflow',
    'port': '5431'  # NOTE: already have postgres running locally...
}

SEBFLOW_HOME = None
DAGS_FOLDER = None
SQL_ALCHEMY_CONN = None

engine = None
Session = None


def configure_vars():
    global SEBFLOW_HOME
    global DAGS_FOLDER
    global SQL_ALCHEMY_CONN

    SEBFLOW_HOME = os.environ.get('SEBFLOW_HOME')

    if SEBFLOW_HOME is None:
        print('could not find environment variable SEBFLOW_HOME')
        sys.exit(1)

    DAGS_FOLDER = os.path.join(SEBFLOW_HOME, 'dags')

    if not os.path.exists(DAGS_FOLDER):
        print('Could not locate dag folder in %s' % settings.SEBFLOW_HOME)
        sys.exit(1)
    SQL_ALCHEMY_CONN = 'postgresql://{user}:{password}@{host}:{port}/{dbname}'.format(**DB_CREDS)


def configure_orm():
    global Session
    global engine
    engine_args = {
        'pool_size': 5,
        'pool_recycle': 3600,
    }

    engine = create_engine(SQL_ALCHEMY_CONN, **engine_args)
    Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


def dispose_orm():
    global engine
    global Session

    if Session:
        Session.remove()
        Session = None
    if engine:
        engine.dispose()
        engine = None


configure_vars()
configure_orm()
