from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}

def _training_model(ti):
    accuracy = uniform(0.1, 10.0)
    print(f'model\'s accuracy: {accuracy}')
    #return accuracy
    ti.xcom_push(key='xcom_var1',value=accuracy)

def _choose_best_model():
    print('choose best model')

with DAG('xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    begin_model = BashOperator(
        task_id='task_0',
        bash_command='sleep 3'
    )
    
    choose_model = PythonOperator(
        task_id='task_1',
        python_callable=_choose_best_model
    )
    
    training_model = PythonOperator(
        task_id='task_2',
        python_callable=_training_model
    )

    #choose_model >> _training_model
    begin_model >> training_model >> choose_model