# utils/config.py
from configparser import ConfigParser
import os

def config(filename="database.ini", section="postgresql"):
    # Получаем абсолютный путь к файлу database.ini
    current_dir = os.path.dirname(__file__)
    full_path = os.path.abspath(os.path.join(current_dir, '..', filename))

    # Создаем парсер
    parser = ConfigParser()

    # Читаем файл конфигурации
    parser.read(full_path)

    db = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            f'Section {section} is not found in the {filename} file at {full_path}.'
        )

    return db

