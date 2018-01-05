import datetime as dt

from sebflow import DAG
from sebflow.operators.python_operator import PythonOperator


def print_date():
    now = dt.datetime.now()
    print now.strftime('%Y/%m/%d %H:%M')


def print_args(arg1, arg2):
    print arg1
    print arg2


def print_kwargs(arg1, arg2, kwarg1=None, kwarg2=None):
    print arg1
    print arg2
    print kwarg1
    print kwarg2


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}

dag = DAG('test', default_args=default_args)

t1 = PythonOperator(
    task_id='print_date',
    dag=dag,
    python_callable=print_date)

t2 = PythonOperator(
    task_id='print_args',
    dag=dag,
    python_callable=print_args,
    op_args=['asdf', 'asdfasdf']
)

t3 = PythonOperator(
    task_id='print_kwargs',
    dag=dag,
    python_callable=print_kwargs,
    op_args=['asdf', 'asdfsdf'],
    op_kwargs={'kwarg1': 'asdfasdf', 'kwarg2': 'asdfasdf'}
)

t2.set_upstream(t1)
t3.set_upstream(t1)

dag.run_now()
