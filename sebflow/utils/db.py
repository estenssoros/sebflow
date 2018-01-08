import contextlib
import os
from functools import wraps

import psycopg2
import sebflow.configuration as conf
from sebflow import settings


def connect_db():
    creds = conf.getsection('db_creds')
    creds.pop('__name__')
    conn = psycopg2.connect(**creds)
    curs = conn.cursor()
    return curs, conn


def initdb(asdf):
    curs, conn = connect_db()

    with open(os.path.join(settings.CONFIG_DIR, conf.get('core', 'create_tables_script')), 'r') as f:
        sql = f.read()

    for query in sql.split(';'):
        query = query.strip()
        if not query:
            continue
        curs.execute(query)
        conn.commit()

    curs.close()
    conn.close()


@contextlib.contextmanager
def create_session():
    """
    Contextmanager that will create and teardown a session.
    """
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
    """
    Function decorator that provides a session if it isn't provided.
    If you want to reuse a session or run the function as part of a
    database transaction, you pass it to the function, if not this wrapper
    will create one and close it for you.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        arg_session = 'session'

        func_params = func.__code__.co_varnames
        session_in_args = arg_session in func_params and \
            func_params.index(arg_session) < len(args)
        session_in_kwargs = arg_session in kwargs

        if session_in_kwargs or session_in_args:
            return func(*args, **kwargs)
        else:
            with create_session() as session:
                kwargs[arg_session] = session
                return func(*args, **kwargs)

    return wrapper


@provide_session
def get_or_create(model, session=None, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    print kwargs
    instance = model(**kwargs)
    session.add(instance)
    session.commit()
    return instance
