import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor
import pymongo
import re




# Создаем функции для задач
def clean_data_1():
    df = pd.read_csv('/opt/airflow/upload/tiktok_google_play_reviews.csv')
    df['thumbsUpCount'] = pd.to_numeric(df['thumbsUpCount'], errors='coerce')
    df = df.dropna(subset=['thumbsUpCount'])
    df.to_csv('/tmp/data.csv')

def clean_data_2():
    df = pd.read_csv('/opt/airflow/upload/tiktok_google_play_reviews.csv')
    df = df.fillna('-')
    df.to_csv('/tmp/data.csv')


def clean_data_3():
    df = pd.read_csv('/tmp/data.csv')
    df = df.sort_values('at')
    df.to_csv('/tmp/data.csv')

def clean_data_4():
    df = pd.read_csv('/tmp/data.csv')
    df['content'] = df['content'].apply(lambda x: re.sub("[^\w\s\.\,\!\?\-]+", '', x))
    df.to_csv('/tmp/data.csv')

def load_to_mongo():
    client = pymongo.MongoClient('mongodb://mongodb:27017/')
    db = client.my_database
    collection = db.my_collection

    df = pd.read_csv('/tmp/data.csv')
    df = df.drop(['Unnamed: 0', 'Unnamed: 0.1', 'Unnamed: 0.1.1'], axis=1)
    df.to_csv('/opt/airflow/upload/tiktok_result.csv')
    data = df.to_dict(orient='records')
    collection.insert_many(data)


# Определяем аргументы DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определяем DAG
with DAG(
        'airflow_task',
        default_args=default_args,
        description='DAG for airflow task',
        schedule_interval=timedelta(days=1),
) as dag:

    file_sensor = FileSensor(
        task_id='file_sensor',
        filepath='/opt/airflow/upload/tiktok_google_play_reviews.csv',
        fs_conn_id='my_file_system',
        poke_interval=30,
        mode='reschedule',
        soft_fail=True
    )

    with TaskGroup("clean_data", tooltip="Tasks for group1") as clean_data:
        clean_data_1 = PythonOperator(
            task_id='clean_data_1',
            python_callable=clean_data_1,
            dag=dag,
        )

        clean_data_2 = PythonOperator(
            task_id='clean_data_2',
            python_callable=clean_data_2,
            dag=dag,
        )

        clean_data_3 = PythonOperator(
            task_id='clean_data_3',
            python_callable=clean_data_3,
            dag=dag,
        )

        clean_data_4 = PythonOperator(
            task_id='clean_data_4',
            python_callable=clean_data_4,
            dag=dag,
        )
        # Определяем зависимости между задачами в группе
        clean_data_1 >> clean_data_2 >> clean_data_3 >> clean_data_4

    load_to_mongo = PythonOperator(
        task_id='load_to_mongo',
        python_callable=load_to_mongo,
        dag=dag,
    )
# Определяем зависимости между задачами
file_sensor >> clean_data >> load_to_mongo