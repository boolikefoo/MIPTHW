# Импортируем библиотеки
import sqlite3
import pandas as pd
import zipfile
import wget


targetFile = 'egrul.json.zip'


# открываем zip архив
with zipfile.ZipFile(targetFile, 'r') as zipobj:
    # получаем список файлов в архиве
    fileList = zipobj.namelist()
    # Читаем данные из файлов архива по списку
    for name in fileList:
        with zipobj.open(name) as file:
            data = file.


