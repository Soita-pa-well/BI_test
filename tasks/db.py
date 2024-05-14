import os
import logging

import psycopg2
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)


def connect_to_database() -> psycopg2.extensions.connection:
    load_dotenv()
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('PASSWORD'),
            host=os.getenv('HOST'),
            port=os.getenv('PORT'),
        )
        logging.info('Подключение к БД успешно')
        return conn
    except Exception as e:
        logging.error(f'Ошибка подключения к БД: {str(e)}')
        raise


def check_table_existence(cur: psycopg2.extensions.cursor) -> bool:
    cur.execute(
        "SELECT EXISTS ("
        "SELECT 1 "
        "FROM information_schema.tables "
        "WHERE table_name = 'institutions'"
        ")"
    )
    table_exists = cur.fetchone()[0]
    return table_exists


def create_table(cur: psycopg2.extensions.cursor,
                 conn: psycopg2.extensions.connection) -> None:
    create_table_query = '''
    CREATE TABLE institutions (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE,
        country TEXT,
        alpha_two_code CHAR(2),
        state_province TEXT,
        type TEXT
    );
    '''
    try:
        cur.execute(create_table_query)
        conn.commit()
        logging.info("Создана таблица 'institutions'")
    except Exception as e:
        logging.error(f'Ошибка при создании таблицы: {str(e)}')
        raise
