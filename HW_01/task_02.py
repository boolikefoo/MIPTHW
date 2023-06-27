# Импортируем библиотеки
import sqlite3
import pandas as pd
import zipfile
import wget
import json
#from tqdm import tqdm


# скачиваем файл
#url = 'https://ofdata.ru/open-data/download/egrul.json.zip'
#targetFile = wget.download(url)
targetFile = 'egrul.json.zip'
# Создаём подключение к DB
connection = sqlite3.connect('HW1.db')

# Создаём курсор
cursor = connection.cursor()

# Создаём таблицу
create_telecom_companies_table = '''
CREATE TABLE IF NOT EXISTS telecom_companies(
    id INTEGER PRIMARY KEY,
    name TEXT,
    inn INTEGER,
    full_name TEXT,
    okved TEXT
    )
'''

cursor.execute(create_telecom_companies_table)
connection.commit()

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
                print(f'Произошла ошибка во время работы: {e} в файле {file}')

# закрываем курсор
cursor.close()

# Закрываем соединение
connection.close()
