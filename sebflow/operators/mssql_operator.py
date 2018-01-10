from sebflow.hook.mssql_hook import MsSqlHook
from sebflow.models import BaseOperator
from sebflow.state import State


class MsSqlOperator(BaseOperator):
    def __init__(self,
                 sql,
                 mssql_conn_id='mssql_default',
                 parameters=None,
                 autocommit=False,
                 database=None,
                 *args,
                 **kwargs):
        super(MsSqlOperator, self).__init__(*args, **kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.parameters = parameters
        self.autocommit = autocommit
        self.database = database

    def execute(self):
        self.state = State.RUNNING
        self.logger.info('Executing: %s', self.sql)
        hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id, schema=self.database)
        hook.run(self.sql, autocommit=self.autocommit, parameters=self.parameters)
