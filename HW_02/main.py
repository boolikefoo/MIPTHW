# подключаем библиотеки
import requests
from bs4 import BeautifulSoup
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from time import sleep
from tqdm import tqdm


# Функция выбора режима парсинга
def parse_mod():
    mod = input('Выберите режим для парсинга: \n1 - парсим с помощью web\n2 - парсим с помощью API\n: ')

    if int(mod) == 1 or int(mod) == 2:
        if int(mod) == 1:
            web_parser_links()
        else:
            api_parsing()
    else:
        print('Указан не корректный режим. Давайте попробуем снова!')
        parse_mod()


# ссылка для парсинга
url_parsing = 'https://hh.ru/search/vacancy'
url_api = 'https://api.hh.ru/vacancies'

# параметры для парсинга
user_agent = {'User-agent': 'Mozilla/5.0'}
url_params = {
    'text': 'middle python developer',
    'area': '1',
    'per_page': 100,
    'page': 0
}

# заголовки
user_agent = {'User-agent': 'Mozilla/5.0'}

# Создаём подключение к DB
engine = create_engine('sqlite:///hw2.db')


# создаём базовый класс
class Base(DeclarativeBase):
    pass


# создаём собственный класс с атрибутами для отображения в DB
class Vacancies(Base):
    # Название таблицы в DB
    __tablename__ = 'vacancies'

    # Столбцы таблицы
    id: Mapped[int] = mapped_column(primary_key=True)
    company_name: Mapped[str]
    position: Mapped[str] 
    job_description: Mapped[str]
    key_skills: Mapped[str]

    def __repr__(self):
        return (f"{self.id}, {self.company_name}, {self.position}, {self.job_description}, {self.key_skills}")


# создаём таблицу vacancies
Base.metadata.create_all(engine)


# Функция проверки наличия данных
def data_check(data):
    try:
        return data.text
    except Exception as e:
        print(f'Ошибка: {e}')
        data = 'Empty'
        return data


# Проверяем ответ сервера на запрос
def parse_data(url, user_agent, url_params, mod='web'):
    result = requests.get(url, headers=user_agent, params=url_params)
    if result.status_code == 200:
        if mod == 'api':
            data = result.json()
            return data
        else:
            soup = BeautifulSoup(result.content.decode(), 'html.parser')
        return soup
    else:
        print(f'Сервер вернул ошибку: {result.status_code}')


# Функция для получения списка ссылок на страницу с вакансией (web парсинг)
def web_parser_links():
    soup = parse_data(url_parsing, user_agent, url_params)
    links = soup.find_all('a', attrs={'class': 'serp-item__title'})

    with Session(engine) as session:

        for link in tqdm(links):
            try:
                data = web_parser_page(link.attrs.get('href'))
                session.add(data)
            except Exception as e:
                print(f'Произошла ошибка: {e}')
        try:
            session.commit()
            print('Данные успешно записаны в базу данных')
        except Exception as e:
            print(f'Произошла ошибка при попытке записи данных: {e}')

        session.close()
        engine.dispose()


# функция парсинга страницы вакансии (web парсинг)
def web_parser_page(url):
    key_skills_list = []

    page_soup = parse_data(url, user_agent, url_params)

    vacancy_title = page_soup.find('h1', attrs={'data-qa': 'vacancy-title'})
    company_name = page_soup.find('span', attrs={'class': 'vacancy-company-name'})
    vacancy_description = page_soup.find('div', attrs={'data-qa': 'vacancy-description'})
    key_skills = page_soup.find_all('span', attrs={'data-qa': 'bloko-tag__text'})

    if key_skills:
        for skill in key_skills:
            key_skills_list.append(skill.text)
    else:
        key_skills_list = 'Empty'
        print('По данной вакансии нет ключевых навыков.')

    company_name = data_check(company_name)
    position = data_check(vacancy_title)
    job_description = data_check(vacancy_description)

    data = Vacancies(
        company_name=company_name,
        position=position,
        job_description=job_description,
        key_skills=str(key_skills_list)
        )
    return data


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


parse_mod()
