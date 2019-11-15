from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import python_operator
from datetime import datetime, timedelta
#from mypipe import data_retrieval
from mypipe.execute import core_get_data,core_aggregation,core_db_insert_to_db
import logging

def core_get_datal(**kwargs):
    import logging
    logging.info('Hello World! in core_get_datal')
    
    
def core_aggregationl(**kwargs):
    logging.info('Hello World! in core_aggregationl')


def core_db_insert_to_dbl(**kwargs):
    logging.info('Hello World! in core_db_insert_to_dbl') 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 16),
    
}

dag = DAG('rate_pop', default_args=default_args, schedule_interval=timedelta(days=1))

# t1, t2  tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag)
    
    
get_data = python_operator.PythonOperator(
    task_id='get_data',
    python_callable=core_get_data,
    #retries=0,
    provide_context=True,
    dag=dag
)


aggregation = python_operator.PythonOperator(
    task_id='aggregation',
    python_callable=core_aggregation,
    #retries=0,
    provide_context=True,
    dag=dag
)


db_insert_to_db = python_operator.PythonOperator(
    task_id='db_insert_to_db',
    python_callable=core_db_insert_to_db,
    #retries=0,
    provide_context=True,
    dag=dag
)
    
    


t1 >> get_data>>aggregation>>db_insert_to_db>>t2
