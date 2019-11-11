import airflow
from airflow.example_dags.subdags.subdag import subdag
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

DAG_NAME = 'subdag_operator'

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=args,
    schedule_interval="@once",
)

start = DummyOperator(
    task_id='start',
    default_args=args,
    dag=dag,
)

section_1 = SubDagOperator(
    task_id='section-1',
    subdag=subdag(DAG_NAME, 'section-1', args),
    default_args=args,
    dag=dag,
)

some_other_task = DummyOperator(
    task_id='some-other-task',
    default_args=args,
    dag=dag,
)

section_2 = SubDagOperator(
    task_id='section-2',
    subdag=subdag(DAG_NAME, 'section-2', args),
    default_args=args,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    default_args=args,
    dag=dag,
)

start >> section_1 >> some_other_task >> section_2 >> end