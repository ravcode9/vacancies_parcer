from utils.utils import (
    terminate_connections, create_database, create_tables, insert_data_into_tables, show_higher_salary_vacancies,
    show_all_vacancies, show_keyword_vacancies, show_companies_and_vacancies, show_avg_salary
)
from classes.db_manager import DBManager


def main():
    """
        Основная функция программы, предоставляющая интерфейс для работы с базой данных вакансий.

        Пользователь может обновить базу данных, выбрать действия для отображения информации и завершить выполнение.

    """
    print("Хотите ли вы обновить базу данных перед просмотром? (ДА/НЕТ)")
    update_db_choice = input()

    if update_db_choice.lower() == 'да':
        db_name = "course_work_5"
        terminate_connections(db_name)
        create_database(db_name)
        create_tables(db_name)
        insert_data_into_tables(db_name)
        print("База успешно обновлена.")

    db_name = "course_work_5"
    db = DBManager(db_name)

    while True:
        print("Выберите вариант действия для отображения информации по базе данных:")
        print("1. Показать компании и количество вакансий у каждой")
        print("2. Показать все вакансии")
        print("3. Показать среднюю зарплату по вакансиям")
        print("4. Показать вакансии с зарплатой выше среднего")
        print("5. Показать вакансии по ключевому слову")

        choice = input("Введите номер варианта (или '0' для выхода): ")

        if choice == '1':
            show_companies_and_vacancies(db)
        elif choice == '2':
            show_all_vacancies(db)
        elif choice == '3':
            show_avg_salary(db)
        elif choice == '4':
            show_higher_salary_vacancies(db)
        elif choice == '5':
            keyword = input("Введите ключевое слово: ")
            show_keyword_vacancies(db, keyword)
        elif choice == '0':
            break
        else:
            print("Некорректный ввод. Пожалуйста, выберите правильный вариант.")


if __name__ == "__main__":
    main()