import time

import sebflow.configuration as conf
from sebflow import DAG
from sebflow.exceptions import SebOnPurposeError
from sebflow.operators.mssql_operator import MsSqlOperator
from sebflow.operators.python_operator import PythonOperator
from sebflow.operators.slack_operator import SlackAPIPostOperator


def sleep1():
    time.sleep(1)


def sleep5(*args, **kwargs):
    time.sleep(5)


def fail_on_purpose(arg1, arg2):
    raise SebOnPurposeError("Failed on purpose ya'll")


def do_nothing(arg1, arg2, kwarg1=None, kwarg2=None):
    pass


dag = DAG(
    'test',
    schedule_interval='@daily',

)

t1 = PythonOperator(
    task_id='task1',
    dag=dag,
    python_callable=sleep1)

t2 = PythonOperator(
    task_id='task12',
    dag=dag,
    python_callable=fail_on_purpose,
    op_args=['asdf', 'asdfasdf']
)

t3 = PythonOperator(
    task_id='task13',
    dag=dag,
    python_callable=sleep5,
    op_args=['asdf', 'asdfsdf'],
    op_kwargs={'kwarg1': 'asdfasdf', 'kwarg2': 'asdfasdf'}
)

t4 = PythonOperator(
    task_id='task121',
    dag=dag,
    python_callable=sleep1,
)

t5 = PythonOperator(
    task_id='task122',
    dag=dag,
    python_callable=sleep1,
)

t6 = PythonOperator(
    task_id='task1212',
    dag=dag,
    python_callable=sleep1,
)

t7 = PythonOperator(
    task_id='task131',
    dag=dag,
    python_callable=sleep1,
)

t8 = PythonOperator(
    task_id='task1311',
    dag=dag,
    python_callable=sleep1,
)
t9 = PythonOperator(
    task_id='task1_1311',
    dag=dag,
    python_callable=sleep1,
)

tsql = MsSqlOperator(
    task_id='test_mssql',
    dag=dag,
    sql='select * from sebflow.dbo.task',
    mssql_conn_id='densql'
)

t10 = SlackAPIPostOperator(
    task_id='test_slack',
    dag=dag,
    channel='denver_notifications',
    token=conf.get('core', 'slack_token')
)

t2.set_upstream(t1)
t3.set_upstream(t1)
t4.set_upstream(t2)
t5.set_upstream(t2)
t6.set_upstream(t4)
t7.set_upstream(t3)
t8.set_upstream(t7)
t9.set_upstream([t1, t3, t5])
tsql.set_upstream(t1)
t10.set_upstream(tsql)

if __name__ == '__main__':
    dag.tree_view()
    dag.run()
    dag.tree_view()
