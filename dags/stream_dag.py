
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from src.producer import create_stream

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': True,
}

dag = DAG(
    dag_id='stream_DAG',
    default_args=args,
    schedule_interval= '@hourly',
    catchup=False
)


create_stream_task = PythonOperator(
    task_id='create_stream_task',
    python_callable=create_stream,
    provide_context=True,
    dag=dag,
)


