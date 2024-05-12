import os
import psycopg2
from dotenv import load_dotenv


def connect_to_database():
    load_dotenv()
    conn = psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST'),
        port=os.getenv('PORT'),
    )
    return conn


def check_table_existence(cur):
    cur.execute(
        "SELECT EXISTS ("
        "SELECT 1 "
        "FROM information_schema.tables "
        "WHERE table_name = 'institutions'"
        ")"
    )
    table_exists = cur.fetchone()[0]
    return table_exists


def create_table(cur, conn):
    create_table_query = '''
    CREATE TABLE institutions (
        id SERIAL PRIMARY KEY,
        name TEXT,
        country TEXT,
        alpha_two_code CHAR(2),
        state_province TEXT,
        type TEXT
    );
    '''
    cur.execute(create_table_query)
    conn.commit()
