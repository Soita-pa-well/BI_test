from db import (connect_to_database,
                check_table_existence,
                create_table)
import psycopg2.extensions


def connect_and_create_table() -> psycopg2.extensions.connection:
    conn = connect_to_database()
    cur = conn.cursor()
    if not check_table_existence(cur):
        create_table(cur, conn)
    return conn

    # cur.close()
    # conn.close()
