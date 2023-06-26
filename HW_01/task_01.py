# импортируем библиотеки
import pandas as pd
import sqlite3
from zipfile import ZipFile
import wget


# Скачиваем данные
try:
    url = 'https://ofdata.ru/open-data/download/okved_2.json.zip'
    filename = wget.download(url)
except Exception as e:
    print(f'Не удаётся загрузить файл: {e}')

# Распаковываем архив
with ZipFile(filename, 'r') as zipobj:
    zipobj.extractall()
jsonFile = 'okved_2.json'
# Читаем JSON файл
dfj = pd.read_json(jsonFile)
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

# записываем данные в таблицу
dfj.to_sql('okved', connection, if_exists='append', index=False)

# закрываем соединение с DB
cursor.close()
connection.close()
