from __future__ import print_function
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import python_operator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 4, 14),
    'end_date': datetime(2019, 4, 14)    
}

dag = DAG('multi_variable_options', 
    schedule_interval="@once", 
    default_args=default_args)


start = DummyOperator(
    task_id="start",
    dag=dag
)

def cals():
        import logging
        var = Variable.get("var2")
        nv   = 5 + int(var)
        logging.info(nv)
# You can directly use a variable from a jinja template
## {{ var.value.<variable_name> }}

t1 = BashOperator(
    task_id="get_variable_value",
    bash_command='echo {{ var.value.var3 }} ',
    dag=dag,
)

t2 = python_operator.PythonOperator(
      task_id='comput',
      python_callable=cals,
      dag=dag,
)

start >> t1 >> t2