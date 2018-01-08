import logging
import os

import pendulum
import sebflow.configuration as conf
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from logging_config import configure_logging

log = logging.getLogger(__name__)

TIMEZONE = pendulum.timezone('UTC')

try:
    tz = conf.get('core', 'default_timezone')
    if tz == 'system':
        TIMEZONE = pendulum.local_timezone()
    else:
        TIMEZONE = pendulum.timezone(tz)
except:
    pass

CONFIG_DIR = os.path.join(os.path.dirname(__file__), 'config_files')

HEADER = '''
   _____ __________  ________    ____ _       __
  / ___// ____/ __ )/ ____/ /   / __ \ |     / /
  \__ \/ __/ / __  / /_  / /   / / / / | /| / /
 ___/ / /___/ /_/ / __/ / /___/ /_/ /| |/ |/ /
/____/_____/_____/_/   /_____/\____/ |__/|__/

'''

LOGGING_LEVEL = logging.INFO

SEBFLOW_HOME = None
SQL_ALCHEMY_CONN = None

engine = None
Session = None


def configure_vars():
    global SEBFLOW_HOME
    global SQL_ALCHEMY_CONN
    SEBFLOW_HOME = os.path.expanduser(conf.get('core', 'SEBFLOW_HOME'))
    DB_CREDS = conf.getsection('db_creds')
    SQL_ALCHEMY_CONN = conf.get('core', 'SQL_ALCHEMY_CONN').format(**DB_CREDS)


def configure_orm(disable_connection_pool=False):
    global engine
    global Session
    engine_args = {}

    engine_args['pool_size'] = conf.getint('core', 'SQL_ALCHEMY_POOL_SIZE')
    engine_args['pool_recycle'] = conf.getint('core', 'SQL_ALCHEMY_POOL_RECYCLE')

    engine = create_engine(SQL_ALCHEMY_CONN, **engine_args)
    # reconnect_timeout = conf.getint('core', 'SQL_ALCHEMY_RECONNECT_TIMEOUT')
    # setup_event_handlers(engine, reconnect_timeout)

    Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


def dispose_orm():
    """ Properly close pooled database connections """
    log.debug("Disposing DB connection pool (PID %s)", os.getpid())
    global engine
    global Session

    if Session:
        Session.remove()
        Session = None
    if engine:
        engine.dispose()
        engine = None


configure_vars()
configure_logging()
configure_orm()
