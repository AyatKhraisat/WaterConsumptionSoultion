
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from src.consumer import get_new_data,archive_data
from src.train_model import retrain_model

MODEL_DATA = '/data/retrain_data.csv'
CURRENT_MODEL_PATH = "/models/SARIMAX_model.pkl"





args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': True,
}

dag = DAG(
    dag_id='update_DAG',
    default_args=args,
	schedule_interval='@daily',
	catchup=False,
)


get_new_data_task = PythonOperator(
    task_id='get_new_data',
    python_callable=get_new_data,
    op_kwargs={'data_path': MODEL_DATA},
    dag=dag,
)

retrain_model = PythonOperator(
    task_id='retrain_model',
    python_callable=retrain_model,
    op_kwargs={'data_path': MODEL_DATA,'current_model_path':CURRENT_MODEL_PATH},
    dag=dag,
)

archive_data_task=PythonOperator(
    task_id='archive_data_task',
    python_callable=archive_data,
    op_kwargs={'data_path': MODEL_DATA},
    dag=dag,
)
get_new_data_task >> retrain_model>>archive_data_task
