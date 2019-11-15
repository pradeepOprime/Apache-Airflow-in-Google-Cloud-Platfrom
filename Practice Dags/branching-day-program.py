from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
# Days are provided in the python list
tabDays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 3),
    'email': ['xxx@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
dag = DAG(
    'Weekdays',
    default_args=default_args,
    schedule_interval="@once")
# returns the week day (monday, tuesday, wednesday, thursday etc.)
def get_day(**kwargs):
    kwargs['ti'].xcom_push(key='day', value=datetime.now().weekday())
# returns the name id of the task to launch (task_for_monday, task_for_tuesday, etc.)
def branch(**kwargs):
    return 'task_for_' + tabDays[kwargs['ti'].xcom_pull(task_ids='weekday', key='day')]
# PythonOperator will retrieve and store into "weekday" variable the week day
get_weekday = PythonOperator(
    task_id='weekday',
    python_callable=get_day,
    provide_context=True,
    dag=dag
)
# BranchPythonOperator will use "weekday" variable, and decide which task to launch next
fork = BranchPythonOperator(
    task_id='branching',
    python_callable=branch,
    provide_context=True,
    dag=dag)
# task 1, get the week day
get_weekday.set_downstream(fork)
# One dummy operator for each week day, all branched to the fork
for day in range(0, 6):
    fork.set_downstream(DummyOperator(task_id='task_for_' + tabDays[day], dag=dag))