# импортируем библиотеки
from datetime import datetime, timedelta, sleep
import logging
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlite3
import zipfile
import wget
import json
from sqlalchemy.orm import Session
from sqlalchemy import create_engine


# Настройки DAG
default_args = {
    'owner': 'Piksaykin',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


# параметры для парсинга
url_api = 'https://api.hh.ru/vacancies'
user_agent = {'User-agent': 'Mozilla/5.0'}
url_params = {
    'text': 'middle python developer',
    'area': '1',
    'per_page': 100,
    'page': 0
}

# Настройки логгера
logging.basicConfig(filename='HW_03.log', 
                    filemode='a',
                    encoding='utf-8', 
                    level=logging.DEBUG)

# Функция загрузки файла
def run_data_downloading():
    # загружаем файл
    try:
        url = 'https://ofdata.ru/open-data/download/egrul.json.zip'
        filename = wget.download(url)
        logging.info('Файл загружен')
    except Exception as e:
        logging.error(f'Не удаётся загрузить файл: {e}')


# функция выбора компаний по оквэд
def run_extract_okved():
    # Создаём подключение к DB
    connection = sqlite3.connect('sqlite:///hw3.db')
    targetFile = 'egrul.json.zip'
    # Создаём курсор
    cursor = connection.cursor()

    # открываем zip архив
    with zipfile.ZipFile(targetFile, 'r') as zipobj:
        # получаем список файлов в архиве
        fileList = zipobj.namelist()
        # Читаем данные из файлов архива по списку
        for name in fileList:
            with zipobj.open(name) as file:
                #  десюриализация json
                jfile = json.load(file)

                # Создаём развёрнутый DataFrame из json
                dfn = pd.json_normalize(jfile)

                try:
                    # Переименовываем столбец с основным видом деятельности
                    dfn.rename(columns={'data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД': 'okved'}, inplace=True)
                    # Удаляем NAN строки из фрейма
                    dfn = dfn.dropna(subset=['okved'])
                    # Фильтруем данные по оквэд
                    dfn_filtred = dfn[dfn['okved'].str.contains('^61')][['name', 'inn', 'full_name', 'okved']]
                    # Записываем данные в DB
                    dfn_filtred.to_sql('telecom_companies', connection, if_exists='append', index=False)

                except Exception as e:
                    logging.error(f'Произошла ошибка во время работы: {e} в файле {file}')

    # закрываем курсор
    cursor.close()

    # Закрываем соединение
    connection.close()

# Функция создания баз данных
def run_create_db():
    # Создаём базу данных
    connection = sqlite3.connect('sqlite:///hw3.db')

    cursor = connection.cursor()

    # Создаём таблицу
    create_telecom_companies_table = """
    CREATE TABLE IF NOT EXISTS telecom_companies(
        id INTEGER PRIMARY KEY,
        name TEXT,
        inn INTEGER,
        full_name TEXT,
        okved TEXT
        )
    """
    cursor.execute(create_vacancies_table)

    create_vacancies_table = """
    CREATE TABLE IF NOT EXISTS vacancies(
    id INTAGE,
    company_name TEXT,
    position TEXT,
    job_description TEXT,
    key_skills TEXT
    )

    """
    cursor.execute(create_vacancies_table)

    connection.commit()


# Функция парсинга вакансий
def run_vacancies_parsing():
    engine = create_engine('sqlite:///hw3.db')
    # функция получения списка ссылок для парсинга по API
    def api_parsing():
        data = parse_data(url_api, user_agent, url_params, mod='api')
        data = data.get('items')

        with Session(engine) as session:
            for link in tqdm(data):
                try:
                    vacancy = api_parsing_vacancies(link['url'])
                    sleep(0.2)
                    session.add(vacancy)
                except Exception as e:
                    print(f'Произошла ошибка: {e}')       
            try:
                session.commit()
                print('Данные успешно записаны в базу данных')
            except Exception as e:
                print(f'Произошла ошибка при попытке записи данных: {e}')

            session.close()
            engine.dispose()


    # Функция парсинга вакансии через AIP
    def api_parsing_vacancies(url):
        vacancy = parse_data(url, user_agent, url_params, mod='api')

        data = Vacancies(
            company_name=vacancy.get('employer').get('name'),
            position=vacancy.get('name'),
            job_description=str(vacancy.get('description')),
            key_skills=str(vacancy.get('key_skills'))
        )

        return data
    
    # Функция проверки наличия данных
    def data_check(data):
        try:
            return data.text
        except Exception as e:
            logging.error(f'Ошибка: {e}')
            data = 'Empty'
            return data


    # Проверяем ответ сервера на запрос
    def parse_data(url, user_agent, url_params):
        result = requests.get(url, headers=user_agent, params=url_params)
        if result.status_code == 200:
            data = result.json()
            return data            
        else:
            logging.error(f'Сервер вернул ошибку: {result.status_code}')


# Функция выбора вакансий телеком компаний
def run_sort_vacancies();
    pass


# функция определения ключевых навыков
def run_top_key_skills():
    pass


with DAG(
    dag_id='MIPT_FINAL_HW_2',
    default_args=default_args,
    description='This is final home work',
    start_date=datetime(2023, 8, 11),
    schedule_interval='@daily
) as dag:

    # Загружаем файл егрюл и разархиваруем данные
    create_tables = PythonOperator(
        task_id='create_tables'
        python_callable=run_create_db 
    )

    data_downloading = PythonOperator(
        task_id='data_downloading',
        python_callable=run_data_downloading
    )

    extract_okved = PythonOperator(
        task_id='extract_okved'
        python_callable=run_extract_okved 
    )

    vacancies_parsing= PythonOperator(
        task_id='vacancies_parsing'
        python_callcable=run_vacancies_parsing 
    )

    sort_vacancies = PythonOperator(
        task_id='sort_vacancies'
        python_callable=run_sort_vacancies 
    )

    top_key_skills = PythonOperator(
        task_id='top_key_skills'
        python_callable=run_top_key_skills
    )


    create_tables >> data_downloading >> extract_okved
    vacancies_parsing >> sort_vacancies >> top_key_skills

