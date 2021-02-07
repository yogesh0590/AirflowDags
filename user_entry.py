from airflow.models import DAG
from datetime import datetime
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

default_args = {
'start_date' : datetime(2020,1,1)
}
with DAG('user_processing',schedule_interval='@daily', 
        default_args=default_args, 
        catchup=False) as dag:
        
    creating_table=SqliteOperator(
        task_id='creating_table',
        sqlite_conn_id='db_sqlite',
        sql='''
            CREATE TABLE USERS(
            userid INTERGER PRIMARY KEY AUTOINCREMENT,
            firstname TEXT null,
            lastname TExT null,
            country TEXT null
            );
            '''
    )
	