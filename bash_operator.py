from datetime import timedelta

from airflow import DAG
#from airflow.operators.bash import BashOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='example_bash_operator',
    default_args=args,
    schedule_interval='* * * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
)



# [START howto_operator_bash]
run_this = BashOperator(
    task_id='run_after_loop',
    bash_command='echo 1',
    dag=dag,
)

run_this_last1 = BashOperator(
    task_id='run_this_last1',
    bash_command='echo 2',
    dag=dag,
)

run_this_last2 = BashOperator(
    task_id='run_this_last2',
    bash_command='echo 3',
    dag=dag,
)


run_final = DummyOperator(
    task_id='run_this_final',
    bash_command='echo 4',
    dag=dag,
)
# [END howto_operator_bash]

run_this >> run_this_last1  >> run_this_last2 >> run_final

#for i in range(3):
#    task = BashOperator(
#        task_id='runme_' + str(i),
#        bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
#        dag=dag,
#    )
#    task >> run_this
#
## [START howto_operator_bash_template]
#also_run_this = BashOperator(
#    task_id='also_run_this',
#    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
#    dag=dag,
#)
## [END howto_operator_bash_template]
#also_run_this >> run_this_last

if __name__ == "__main__":
    dag.cli()