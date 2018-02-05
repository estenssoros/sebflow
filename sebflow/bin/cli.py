#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import reprlib
from collections import namedtuple
from urlparse import urlunparse

from sqlalchemy.orm import exc

import tabulate

from .. import configuration as conf
from .. import settings
from ..models import Connection
from ..utils import db as db_utils
from ..www.app import cached_app


def webserver(args):
    print(settings.HEADER)
    access_logfile = args.access_logfile or conf.get('webserver', 'access_logfile')
    error_logfile = args.error_logfile or conf.get('webserver', 'error_logfile')
    num_workers = args.workers or conf.get('webserver', 'workers')
    worker_timeout = args.worker_timeout or conf.get('webserver', 'web_server_worker_timeout')
    ssl_cert = args.ssl_cert or conf.get('webserver', 'web_server_ssl_cert')
    ssl_key = args.ssl_key or conf.get('webserver', 'web_server_ssl_key')


def initdb(args):  # noqa
    print("DB: " + repr(settings.engine.url))
    db_utils.initdb(args)
    print("Done.")


alternative_conn_specs = [
    'conn_type',
    'conn_host',
    'conn_login',
    'conn_password',
    'conn_schema',
    'conn_port'
]


def connections(args):
    if args.list:
        invalid_args = list()
        for arg in ['conn_id', 'conn_uri', 'conn_extra'] + alternative_conn_specs:
            if getattr(args, arg) is not None:
                invalid_args.append(arg)
        if invalid_args:
            msg = ('\n\tThe following args are not compatible with the --list flag: {invalid!r}\n')
            msg = msg.format(invalid=invalid_args)
            print(msg)
            return

        session = settings.Session()
        conns = session.query(Connection.conn_id, Connection.conn_type,
                              Connection.host, Connection.port,
                              Connection.is_encrypted,
                              Connection.is_extra_encrypted,
                              Connection.extra).all()
        conns = [map(reprlib.repr, conn) for conn in conns]
        print(tabulate(conns, ['Conn Id', 'Conn Type', 'Host', 'Port',
                               'Is Encrypted', 'Is Extra Encrypted', 'Extra'],
                       tablefmt="fancy_grid"))
        return

    if args.delete:
        invalid_args = list()
        for arg in ['conn_uri', 'conn_extra'] + alternative_conn_specs:
            if getattr(args, arg) is not None:
                invalid_args.append(arg)

        if invalid_args:
            msg = ('\n\tThe following args are not compatible with the ' +
                   '--delete flag: {invalid!r}\n')
            msg = msg.format(invalid=invalid_args)
            print(msg)
            return

        if args.conn_id is None:
            print('\n\tTo delete a connection, you Must provide a value for ' +
                  'the --conn_id flag.\n')
            return

        session = settings.Session()
        try:
            to_delete = (session
                         .query(Connection)
                         .filter(Connection.conn_id == args.conn_id)
                         .one())
        except exc.NoResultFound:
            msg = '\n\tDid not find a connection with `conn_id`={conn_id}\n'
            msg = msg.format(conn_id=args.conn_id)
            print(msg)
            return
        except exc.MultipleResultsFound:
            msg = ('\n\tFound more than one connection with ' +
                   '`conn_id`={conn_id}\n')
            msg = msg.format(conn_id=args.conn_id)
            print(msg)
            return
        else:
            deleted_conn_id = to_delete.conn_id
            session.delete(to_delete)
            session.commit()
            msg = '\n\tSuccessfully deleted `conn_id`={conn_id}\n'
            msg = msg.format(conn_id=deleted_conn_id)
            print(msg)
        return

    if args.add:
        missing_args = list()
        invalid_args = list()
        if not args.conn_id:
            missing_args.append('conn_id')
        if args.conn_uri:
            for arg in alternative_conn_specs:
                if getattr(args, arg) is not None:
                    invalid_args.append(arg)
        elif not args.conn_type:
            missing_args.append('conn_uri or conn_type')
        if missing_args:
            msg = ('\n\tThe following args are required to add a connection:' +
                   ' {missing!r}\n'.format(missing=missing_args))
            print(msg)
        if invalid_args:
            msg = ('\n\tThe following args are not compatible with the ' +
                   '--add flag and --conn_uri flag: {invalid!r}\n')
            msg = msg.format(invalid=invalid_args)
            print(msg)
        if missing_args or invalid_args:
            return

        if args.conn_uri:
            new_conn = Connection(conn_id=args.conn_id, uri=args.conn_uri)
        else:
            new_conn = Connection(conn_id=args.conn_id, conn_type=args.conn_type, host=args.conn_host,
                                  login=args.conn_login, password=args.conn_password, schema=args.conn_schema, port=args.conn_port)
        if args.conn_extra is not None:
            new_conn.set_extra(args.conn_extra)

        session = settings.Session()
        if not (session
                .query(Connection)
                .filter(Connection.conn_id == new_conn.conn_id).first()):
            session.add(new_conn)
            session.commit()
            msg = '\n\tSuccessfully added `conn_id`={conn_id} : {uri}\n'
            msg = msg.format(conn_id=new_conn.conn_id, uri=args.conn_uri or urlunparse((args.conn_type, '{login}:{password}@{host}:{port}'.format(
                login=args.conn_login or '', password=args.conn_password or '', host=args.conn_host or '', port=args.conn_port or ''), args.conn_schema or '', '', '', '')))
            print(msg)
        else:
            msg = '\n\tA connection with `conn_id`={conn_id} already exists\n'
            msg = msg.format(conn_id=new_conn.conn_id)
            print(msg)

        return


Arg = namedtuple('Arg', ['flags', 'help', 'action', 'default', 'nargs', 'type', 'choices', 'metavar'])
Arg.__new__.__defaults__ = (None, None, None, None, None, None, None)


class CLIFactory(object):
    args = {
        'list_connections': Arg(('-l', '--list'), help='List all connections', action='store_true'),
        'add_connection': Arg(('-a', '--add'), help='Add a connection', action='store_true'),
        'delete_connection': Arg(('-d', '--delete'), help='Delete a connection', action='store_true'),
        'conn_id': Arg(('--conn_id',), help='Connection id ,require to add/delete a connection', type=str),
        'conn_uri': Arg(('--conn_uri',), help='Connection URI, required to add a connection without conn_type', type=str),
        'conn_type': Arg(('--conn_type',), help='Connection type, required to add a connection without conn_uri', type=str),
        'conn_host': Arg(('--conn_host',), help='Connection host, optional when adding a connection', type=str),
        'conn_login': Arg(('--conn_login',), help='Connection login, optional when adding a connection', type=str),
        'conn_password': Arg(('--conn_password',), help='Connection password, optional when adding a connection', type=str),
        'conn_schema': Arg(('--conn_schema',), help='Connection schema, optional when adding a connection', type=str),
        'conn_port': Arg(('--conn_port',), help='Connection port, optional when adding a connection', type=str),
        'conn_extra': Arg(('--conn_extra',), help='Connection `Extra` field, optional when adding a connection', type=str),
        # WEBSERVER
        'port': Arg(
            ('-p', '--port'),
            default=conf.get('webserver', 'WEB_SERVER_PORT'),
            type=int,
            help='The Port on which to run the server'),
        'workers': Arg(
            ("-w", "--workers"),
            default=conf.get('webserver', 'WORKERS'),
            type=int,
            help="Number of workers to run the webserver on"),
        'workerclass': Arg(
            ("-k", "--workerclass"),
            default=conf.get('webserver', 'WORKER_CLASS'),
            choices=['sync', 'eventlet', 'gevent', 'tornado'],
            help="The worker class to use for Gunicorn"),
        'worker_timeout': Arg(
            ("-t", "--worker_timeout"),
            default=conf.get('webserver', 'WEB_SERVER_WORKER_TIMEOUT'),
            type=int,
            help="The timeout for waiting on webserver workers"),
        'hostname': Arg(
            ("-hn", "--hostname"),
            default=conf.get('webserver', 'WEB_SERVER_HOST'),
            help="Set the hostname on which to run the web server"),
        'pid': Arg(
            ("--pid",),
            "PID file location",
            nargs='?'),
        'daemon': Arg(
            ("-D", "--daemon"),
            "Daemonize instead of running in the foreground",
            "store_true"),
        'stdout': Arg(
            ("--stdout",), "Redirect stdout to this file"),
        'stderr': Arg(
            ("--stderr",), "Redirect stderr to this file"),
        'access_logfile': Arg(
            ("-A", "--access_logfile"),
            default=conf.get('webserver', 'ACCESS_LOGFILE'),
            help="The logfile to store the webserver access log. Use '-' to print to stderr."),
        'error_logfile': Arg(
            ("-E", "--error_logfile"),
            default=conf.get('webserver', 'ERROR_LOGFILE'),
            help="The logfile to store the webserver error log. Use '-' to print to stderr."),
        'log_file': Arg(
            ("-l", "--log-file"),
            "Location of the log file"),
        'ssl_cert': Arg(
            ("--ssl_cert",),
            default=conf.get('webserver', 'WEB_SERVER_SSL_CERT'),
            help="Path to the SSL certificate for the webserver"),
        'ssl_key': Arg(
            ("--ssl_key",),
            default=conf.get('webserver', 'WEB_SERVER_SSL_KEY'),
            help="Path to the key to use with the SSL certificate"),
        'debug': Arg(
            ("-d", "--debug"),
            "Use the server that ships with Flask in debug mode",
            "store_true"),
    }
    subparsers = (
        {
            'func': initdb,
            'help': "Initialize testing databases",
            'args': tuple(),
        },
        {
            'func': connections,
            'help': "List/Add/Delete connections",
            'args': ('list_connections', 'add_connection', 'delete_connection',
                     'conn_id', 'conn_uri', 'conn_extra') + tuple(alternative_conn_specs),
        },
        {
            'func': webserver,
            'help': 'Start a Sebflow webserver isntance',
            'args': ('port', 'workers', 'workerclass', 'worker_timeout', 'hostname',
                     'pid', 'daemon', 'stdout', 'stderr', 'access_logfile',
                     'error_logfile', 'log_file', 'ssl_cert', 'ssl_key', 'debug'),
        }
    )
    subparsers_dict = {sp['func'].__name__: sp for sp in subparsers}

    @classmethod
    def get_parser(cls, dag_parser=False):
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(help='sub-command help', dest='subcommand')
        subparsers.required = True

        subparser_list = cls.dag_subparsers if dag_parser else cls.subparsers_dict.keys()
        for sub in subparser_list:
            sub = cls.subparsers_dict[sub]
            sp = subparsers.add_parser(sub['func'].__name__, help=sub['help'])
            for arg in sub['args']:
                if 'dag_id' in arg and dag_parser:
                    continue
                arg = cls.args[arg]
                kwargs = {
                    f: getattr(arg, f)
                    for f in arg._fields if f != 'flags' and getattr(arg, f)}
                sp.add_argument(*arg.flags, **kwargs)
            sp.set_defaults(func=sub['func'])
        return parser


def get_parser():
    return CLIFactory.get_parser()
