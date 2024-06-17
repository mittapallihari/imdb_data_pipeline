from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta
from common.etl import load_data, transform_data
from callbacks.failure_callbacks import task_failure_callback


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_failure_callback,  # Set the default failure callback
}

dag = DAG(
    'imdb_data_pipeline',
    default_args=default_args,
    description='IMDb ETL data pipeline',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Define variables
INPUT_PATH = Variable.get("input_path", default_var="data/input/")
OUTPUT_PATH = Variable.get("output_path", default_var="data/output/transformed.parquet")
DB_CONN = Variable.get("db_conn", default_var="sqlite:///census.sqlite")

with dag:

    # Sensor task to check if the file exists
    title_file_sensor_task = FileSensor(
        task_id='file_sensor_task',
        filepath=INPUT_PATH + "title.basics.sample.tsv",
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=600,
        mode='poke',
    )

    ratings_file_sensor_task = FileSensor(
        task_id='file_sensor_task',
        filepath=INPUT_PATH + "title.ratings.sample.tsv",
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=600,
        mode='poke',
    )

    principals_file_sensor_task = FileSensor(
        task_id='file_sensor_task',
        filepath=INPUT_PATH + "title.principals.sample.tsv",
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=600,
        mode='poke',
    )

    name_file_sensor_task = FileSensor(
        task_id='file_sensor_task',
        filepath=INPUT_PATH + "name.basics.sample.tsv",
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=600,
        mode='poke',
    )

    # Transform data task
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={'input_path': INPUT_PATH,
                   'output_path': OUTPUT_PATH}
    )

    # Load to DB task
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_kwargs={'db_conn': DB_CONN,
                   'transformed_file_path': OUTPUT_PATH}
    )

    [title_file_sensor_task,
     ratings_file_sensor_task,
     principals_file_sensor_task,
     name_file_sensor_task] >> transform_task >> load_task