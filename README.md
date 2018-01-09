# sebflow
Airflow is awesome

But it's confusing.

- So many logs
- How do I run anything?
- Did I just run something?
- I think I just ran something...
- Really, too many logs

And bulky.

- You have to launch a webserver and a scheduler to run a dag.

And it doesn't work on Windows...

##### Sebflow is an Airflow implementation that runs locally on both Windows and Linux, only uses PythonOperators, and allows you to use your computer's scheduling programs to run a dag.

SebFlow is ripped almost entirely from the Incubator-Airflow project: a team of incredible programmers. I have learned so much from their code.

```
import time

from sebflow.models import DAG
from sebflow.operators.python_operator import PythonOperator
from sebflow.exceptions import SebOnPurposeError


def sleep1():
    time.sleep(1)


def sleep5(*args, **kwargs):
    time.sleep(5)


def fail_on_purpose(arg1, arg2):
    raise SebOnPurposeError()


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


t2.set_upstream(t1)
t3.set_upstream(t1)

if __name__ == '__main__':
    dag.run()

```


Crontab syntax:
```
*     *     *   *    *
-     -     -   -    -
|     |     |   |    |
|     |     |   |    +----- day of week (0 - 6) (Sunday=0)
|     |     |   +------- month (1 - 12)
|     |     +--------- day of        month (1 - 31)
|     +----------- hour (0 - 23)
+------------- min (0 - 59)
```
## Todo:
- add notifications
  - apply defaults to python_operator etc.
- check for failed dags/tasks in schedule
  - make scheduled interval work
- add sebflow_home implementation
  - allows user to configure their own stuff
