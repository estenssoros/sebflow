#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import reprlib
from collections import namedtuple
from urlparse import urlunparse

from sqlalchemy.orm import exc

from .. import settings
from ..models import Connection
from ..utils import db as db_utils


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
