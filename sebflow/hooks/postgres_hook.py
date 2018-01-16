from contextlib import closing
from contextlib.hooks.dbapi_hook import DbApiHook

import psycopg2
import psycopg2.extensions


class PostgresHook(DbApiHook):
    """
    Interact with Postgres.
    You can specify ssl parameters in the extra field of your connection
    as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc}``.

    Note: For Redshift, use keepalives_idle in the extra connection parameters
    and set it to less than 300 seconds.
    """
    conn_name_attr = 'postgres_conn_id'
    default_conn_name = 'postgres_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super(PostgresHook, self).__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def get_conn(self):
        conn = self.get_connection(self.postgres_conn_id)
        conn_args = dict(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            dbname=self.schema or conn.schema,
            port=conn.port)
        # check for ssl parameters in conn.extra
        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name in ['sslmode', 'sslcert', 'sslkey',
                            'sslrootcert', 'sslcrl', 'application_name',
                            'keepalives_idle']:
                conn_args[arg_name] = arg_val

        psycopg2_conn = psycopg2.connect(**conn_args)
        return psycopg2_conn

    def copy_expert(self, sql, filename, open=open):
        '''
        Executes SQL using psycopg2 copy_expert method
        Necessary to execute COPY command without access to a superuser
        '''
        f = open(filename, 'w')
        with closing(self.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.copy_expert(sql, f)

    @staticmethod
    def _serialize_cell(cell, conn):
        """
        Postgresql will adapt all arguments to the execute() method internally,
        hence we return cell without any conversion.

        See http://initd.org/psycopg/docs/advanced.html#adapting-new-types for
        more information.

        :param cell: The cell to insert into the table
        :type cell: object
        :param conn: The database connection
        :type conn: connection object
        :return: The cell
        :rtype: object
        """
        return cell
