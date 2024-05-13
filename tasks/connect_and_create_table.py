from db import (connect_to_database,
                check_table_existence,
                create_table)


def connect_and_create_table():
    conn = connect_to_database()
    cur = conn.cursor()
    if not check_table_existence(cur):
        create_table(cur, conn)
    cur.close()
    conn.close()
