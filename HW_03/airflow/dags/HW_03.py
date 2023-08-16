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
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

# Настройки DAG
default_args = {
    'owner': 'Piksaykin',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

# Настройки логгера
logging.basicConfig(filename='HW_03.log', 
                    filemode='a',
                    encoding='utf-8', 
                    level=logging.DEBUG)

# Функция загрузки файла
def data_downlo():
    print("this is test function")


with DAG(
    dag_id='MIPT_FINAL_HW_2',
    default_args=default_args,
    description='This is final home work',
    start_date=datetime(2023, 8, 11),
    schedule_interval='@daily
) as dag:

    # Создаём базу данных
    @task(task_id='create_db')
    def create_db():
        # Создаём базу данных
        connection = sqlite3.connect('hw1.db')

        cursor = connection.cursor()

        # Создаём таблицу
        create_okved_table = """
        CREATE TABLE IF NOT EXISTS okved(
        code TEXT,
        parent_code TEXT,
        section CHAR,
        name TEXT,
        comment TEXT
        )
        """

        cursor.execute(create_okved_table)

        connection.commit()

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

    task_1 = PythonOperator(
        task_id='data_downloading',
        python_callable=data_downloading
    )



    # Task dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # Task dependency method 2
    # task1 >> task2
    # task1 >> task3

    run_task0 >> task_1
    # Task dependency method 3
    # task1 >> [task2, task3]
