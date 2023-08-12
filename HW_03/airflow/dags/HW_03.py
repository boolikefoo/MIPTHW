# импортируем библиотеки
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

import pandas as pd
import sqlite3
from zipfile import ZipFile
import wget
import logging

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

# Настройки DAG
default_args = {
    'owner': 'Piksaykin',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


# Настройки логера
logging.config.fileConfig(fname='configs/logger.conf', disable_existing_loggers=False)
logger = logging.getLogger('customLogger')


# Функция загрузки файла
def data_downlo():
    print("this is test function")


with DAG(
    dag_id='MIPT_FINAL_HW',
    default_args=default_args,
    description='This is final home work',
    start_date=datetime(2023, 8, 11),
    schedule_interval='@daily'
) as dag:

    # Загружаем файл егрюл и разархиваруем данные
    @task(task_id='data_downloading')
    def data_downloading(url):
        try:
            url = 'https://ofdata.ru/open-data/download/okved_2.json.zip'
            filename = wget.download(url)
            logging.info('Файл загружен')
        except Exception as e:
            logging.error(f'Не удаётся загрузить файл: {e}')

            # Распаковываем архив
        with ZipFile(filename, 'r') as zipobj:
            zipobj.extractall()
        jsonFile = 'okved_2.json'

    run_task0 = data_downloading('someurl')

    task1 = PythonOperator(
        task_id='first_task',
        python_callable=testdef
    )

    task2 = PythonOperator(
        task_id='second_task',
        python_callable=testdef
    )

    task3 = PythonOperator(
        task_id='thrid_task',
        python_callable=testdef
    )

    # Task dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # Task dependency method 2
    # task1 >> task2
    # task1 >> task3

    run >> task3 >> task2
    

    # Task dependency method 3
    # task1 >> [task2, task3]
