from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1),
    'provide_context': True
}

dag=DAG(
    dag_id='simple_xcom_DAG',
    default_args=default_args,
)

def push_func(**kwargs):
    message='this is pushed message'
    ti=kwargs['ti']
    ti.xcom_push(key="message",value=message)


def pull_func(**kwargs):
    ti=kwargs['ti']
    pulled_message=ti.xcom_pull(key='message')
    print("pulled message : '%s'" % pulled_message)
    
t1=PythonOperator(
    task_id='push_task',
    python_callable=push_func,
    provide_context=True,
    dag=dag
)

t2=PythonOperator(
    task_id='pull_task',
    python_callable=pull_func,
        provide_context=True,
    dag=dag
)    

t1 >> t2