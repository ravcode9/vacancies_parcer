import psycopg2
from utils.config import config


class DBManager:
    def __init__(self, db_name):
        self.db_name = db_name

    def execute_query(self, query, values=None, query_type="SELECT"):
        """
                Выполняет SQL-запрос к базе данных.
        """

        conn = psycopg2.connect(dbname=self.db_name, **config())
        cur = conn.cursor()

        try:
            if query_type == "SELECT":
                cur.execute(query, values)
                result = cur.fetchall()
            else:
                cur.execute(query, values)
                result = None

            conn.commit()
            return result
        except Exception as e:
            print(f"Error executing query: {e}")
        finally:
            cur.close()
            conn.close()
    def get_companies_and_vacancies_count(self):
        """
                Получает количество вакансий для каждой компании.
        """

        query = """
            SELECT employers.name AS company_name, COUNT(vacancies.id) AS vacancies_count
            FROM employers
            LEFT JOIN vacancies ON employers.id = vacancies.employer
            GROUP BY employers.id, employers.name
        """
        return self.execute_query(query)

    def get_all_vacancies_db(self):
        """
                Получает все вакансии с дополнительной информацией о компании.
        """
        query = """
            SELECT vacancies.id, employers.name AS company_name, vacancies.name AS vacancy_name,
                   vacancies.salary_from, vacancies.salary_to, vacancies.published_at, vacancies.url
            FROM vacancies
            INNER JOIN employers ON vacancies.employer = employers.id
        """
        return self.execute_query(query)

    def get_avg_salary(self):
        query = "SELECT AVG(vacancies.salary_from + vacancies.salary_to) / 2 AS avg_salary FROM vacancies WHERE vacancies.salary_from IS NOT NULL AND vacancies.salary_to IS NOT NULL"
        return self.execute_query(query)

    def get_vacancies_with_higher_salary(self):
        query = """
            SELECT vacancies.id, employers.name AS company_name, vacancies.name AS vacancy_name,
                   vacancies.salary_from, vacancies.salary_to, vacancies.published_at, vacancies.url
            FROM vacancies
            INNER JOIN employers ON vacancies.employer = employers.id
            WHERE vacancies.salary_from > (SELECT AVG(salary_from) FROM vacancies WHERE salary_from > 0)
        """
        return self.execute_query(query)

    def get_vacancies_with_keyword(self, keyword):
        query = """
            SELECT vacancies.id, employers.name AS company_name, vacancies.name AS vacancy_name,
                   vacancies.salary_from, vacancies.salary_to, vacancies.published_at, vacancies.url
            FROM vacancies
            INNER JOIN employers ON vacancies.employer = employers.id
            WHERE vacancies.name ILIKE %s
        """
        return self.execute_query(query, (f'%{keyword}%',), "SELECT")

