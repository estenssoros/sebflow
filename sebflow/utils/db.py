import contextlib
import os
import sys
from functools import wraps

import psycopg2
from sebflow import settings


@contextlib.contextmanager
def create_session():
    session = settings.Session()
    try:
        yield session
        session.expunge_all()
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def provide_session(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        arg_session = 'session'
        func_params = func.__code__.co_varnames
        session_in_args = arg_session in func_params and func_params.index(arg_session) < len(args)
        session_in_kwargs = arg_session in kwargs

        if session_in_args or session_in_kwargs:
            return func(*args, **kwargs)
        else:
            with create_session() as session:
                kwargs[arg_session] = session
                return func(*args, **kwargs)
    return wrapper


def init_db():

    dags = [f for f in os.listdir(settings.DAGS_FOLDER) if f.endswith('.py')]
    if not dags:
        print('No dags present in %s' % dag_folder)
        sys.exit(1)

    conn = psycopg2.connect(**settings.DB_CREDS)
    curs = conn.cursor()

    curs.execute('DROP TABLE IF EXISTS dag;')
    conn.commit()

    curs.execute('''
    CREATE TABLE dag (
        dag_id VARCHAR(250)
        , is_paused BOOLEAN DEFAULT False
        , schedule_interval VARCHAR(10) DEFAULT NULL
        , last_run TIMESTAMP DEFAULT NULL
        , fileloc VARCHAR(2000)
        , PRIMARY KEY (dag_id)
    )
    ''')
    conn.commit()

    curs.execute('DROP TABLE IF EXISTS dag_run')
    conn.commit()

    curs.execute(
        '''
    CREATE TABLE dag_run (
        id SERIAL
        , dag_id VARCHAR(250)
        , execution_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        , state VARCHAR(50)
    )
    ''')

    curs.execute('DROP TABLE IF EXISTS job')
    conn.commit()

    curs.execute(
        '''
    CREATE TABLE job (
        id SERIAL
        , dag_id VARCHAR(250)
        , state VARCHAR(20)
        , job_type VARCHAR(30)
        , start_date TIMESTAMP WITH TIME ZONE
        , end_date TIMESTAMP WITH TIME ZONE
        , latest_heartbeat TIMESTAMP WITH TIME ZONE
        , executor_class VARCHAR(100)
        , hostname VARCHAR(100)
        , unixname VARCHAR(100)
    )
    ''')
    conn.commit()

    conn.close()
    curs.close()
