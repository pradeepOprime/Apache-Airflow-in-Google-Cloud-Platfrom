import datetime

from airflow import models
from airflow.operators import bash_operator


yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())


default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': datetime.datetime(2019, 3, 31),
}

with models.DAG(
        'composer_call_bashoperator_python',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    run_python = bash_operator.BashOperator(
        task_id='run_python3',
        # This example runs a Python script from the data folder to prevent
        # Airflow from attempting to parse the script as a DAG.
        bash_command='python2 /home/airflow/gcs/data/test.py')