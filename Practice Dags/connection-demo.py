
import datetime

from airflow import models
from airflow.contrib.operators import bigquery_operator


yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'composer_sample_connections',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    # [START composer_connections_default]
    task_default = bigquery_operator.BigQueryOperator(
        task_id='task_default_connection',
        bql='SELECT 1', use_legacy_sql=False)
    # [END composer_connections_default]
    # [START composer_connections_explicit]
    task_explicit = bigquery_operator.BigQueryOperator(
        task_id='task_explicit_connection',
        bql='SELECT 1', use_legacy_sql=False,
        # Composer creates a 'google_cloud_default' connection by default.
        bigquery_conn_id='google_cloud_default')
    # [END composer_connections_explicit]
    # [START composer_connections_custom]
    task_custom = bigquery_operator.BigQueryOperator(
        task_id='task_custom_connection',
        bql='SELECT 1', use_legacy_sql=False,
        # Set a connection ID to use a connection that you have created.
        bigquery_conn_id='my_gcp_connection')
    # [END composer_connections_custom]