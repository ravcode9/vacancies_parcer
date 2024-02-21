import psycopg2
from utils.config import config
from classes.hh_parser import HHParser


def terminate_connections(db_name):
    """
        Завершает все активные соединения с указанной базой данных.
    """
    conn = psycopg2.connect(dbname="postgres", **config())
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = %s AND pid <> pg_backend_pid();
    """, (db_name,))

    cur.close()
    conn.close()


def create_database(db_name):
    """
        Создает новую базу данных с указанным именем.
    """
    conn = psycopg2.connect(dbname="postgres", **config())
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
    cur.execute(f"CREATE DATABASE {db_name}")

    cur.close()
    conn.close()

def create_tables(db_name):
    """
        Создает таблицы "employers" и "vacancies" в указанной базе данных.
    """
    conn = psycopg2.connect(dbname=db_name, **config())
    with conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE employers "
                        "("
                        "id int PRIMARY KEY,"
                        "name varchar(255) UNIQUE NOT NULL)")
            cur.execute("CREATE TABLE vacancies "
                        "("
                        "id int PRIMARY KEY,"
                        "name varchar(255) UNIQUE NOT NULL, "
                        "area varchar(255), "
                        "salary_from int,"
                        "salary_to int,"
                        "published_at timestamp,"
                        "url varchar(255),"
                        "employer int REFERENCES employers(id) NOT NULL)")
    conn.close()


def insert_data_into_tables(db_name):
    """
        Заполняет таблицы "employers" и "vacancies" данными из HH API.
    """
    hh = HHParser()
    employers = hh.get_employers()
    vacancies = hh.filter_vacancies()
    conn = psycopg2.connect(dbname=db_name, **config())
    with conn:
        with conn.cursor() as cur:
            for employer in employers:
                cur.execute("""
                    INSERT INTO employers VALUES (%s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """, (employer['id'], employer['name']))
            for vacancy in vacancies:
                cur.execute("""
                    INSERT INTO vacancies VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (name) DO UPDATE SET
                    area = EXCLUDED.area,
                    salary_from = EXCLUDED.salary_from,
                    salary_to = EXCLUDED.salary_to,
                    published_at = EXCLUDED.published_at,
                    url = EXCLUDED.url,
                    employer = EXCLUDED.employer;
                """, (vacancy['id'], vacancy['name'], vacancy['area'],
                      vacancy['salary_from'], vacancy['salary_to'],
                      vacancy['published_at'], vacancy['url'], vacancy['employer']))


def show_companies_and_vacancies(db):
    """
        Выводит информацию о компаниях и количестве их вакансий.
    """
    companies_and_vacancies_count = db.get_companies_and_vacancies_count()
    print("Компании и количество вакансий:")
    for row in companies_and_vacancies_count:
        print(row)

def show_all_vacancies(db):
    """
        Выводит информацию о всех вакансиях.
    """
    all_vacancies = db.get_all_vacancies_db()
    print("\nВсе вакансии:")
    for row in all_vacancies:
        print(row)

def show_avg_salary(db):
    """
        Выводит среднюю зарплату вакансий.
    """
    avg_salary = db.get_avg_salary()
    print("\nСредняя зарплата вакансий:", avg_salary[0][0])

def show_higher_salary_vacancies(db):
    """
        Выводит информацию о вакансиях с зарплатой выше среднего.
    """
    vacancies_with_higher_salary = db.get_vacancies_with_higher_salary()
    print("\nВакансии с зарплатой выше среднего:")
    for row in vacancies_with_higher_salary:
        print(row)

def show_keyword_vacancies(db, keyword):
    """
        Выводит информацию о вакансиях по заданному ключевому слову.
    """
    keyword = keyword.lower()
    vacancies = db.get_vacancies_with_keyword(keyword)

    if not vacancies:
        print(f"К сожалению, по ключевому слову '{keyword}' вакансии обнаружить не удалось.")
    else:
        print(f"\nВакансии по ключевому слову '{keyword}':")
        for row in vacancies:
            print(row)