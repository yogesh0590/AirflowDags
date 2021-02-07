from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.models import DAG
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 5, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='python_branch_operator',
    default_args=default_args,
    schedule_interval='* * * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)

def branch_func(**kwargs):
    if 100 > 2 :
        return 'python_hello'
    return 'Bash_Echo'
    
def helloCode(**kwargs):
    print('Hello from {kw}'.format(kw=kwargs['my_keyword']))

t0 = BashOperator(
task_id='Bash_1',
bash_command='echo 0',
dag=dag
)
  
t2 = PythonOperator(
    task_id='python_hello',
    dag=dag,
    python_callable=helloCode,
    op_kwargs={'my_keyword': 'Airflow'}
)

t1 = BashOperator(
task_id='Bash_Echo',
bash_command='echo 1',
dag=dag
)

branch_op = BranchPythonOperator(
    task_id='branch_op',
    python_callable=branch_func,
    dag=dag
)

t0 >> branch_op >> [t1,t2]