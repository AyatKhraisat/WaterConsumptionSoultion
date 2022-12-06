import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from src.train_model import train_model

INITIAL_DATA = '/data/initial_training_dataset.csv'
CURRENT_MODEL_PATH = "/models/SARIMAX_model.pkl"


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': True,
}

dag = DAG(
    dag_id='initial_model_dag',
    default_args=args,
    schedule_interval='@once',
    catchup=False
)

train_model_task = PythonOperator(
    task_id='train_model_task',
    python_callable=train_model,
    op_kwargs={'data_path': INITIAL_DATA,'model_path':CURRENT_MODEL_PATH},
    dag=dag,
)
