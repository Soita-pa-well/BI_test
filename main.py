# import schedule
# import time
from api import get_info
from db import connect_to_database, check_table_existence, create_table
from constance import UNIVERSE_URL
import logging

logging.basicConfig(level=logging.INFO)

conn = connect_to_database()
cur = conn.cursor()

if not check_table_existence(cur):
    create_table(cur, conn)


def main():
    data = get_info(UNIVERSE_URL)
    for institute in data:
        insitute_type = ''
        if 'Institute' in institute['name']:
            insitute_type = 'Insitite'
        elif 'College' in institute['name']:
            insitute_type = 'College'
        elif 'University' in institute['name']:
            insitute_type = 'University'
        else:
            insitute_type = 'None'

        cur.execute("SELECT * "
                    "FROM institutions "
                    "WHERE name = %s",
                    (institute['name'],)
                    )
        existing_record = cur.fetchone()

        if existing_record:
            (ex_id, existing_name, existing_country, existing_alpha,
             existing_state, existing_type) = existing_record
            
            name_changed = existing_name != institute['name']
            country_changed = existing_country != institute['country']
            alpha_changed = existing_alpha != institute['alpha_two_code']
            state_changed = existing_state != institute['state-province']
            type_changed = existing_type != insitute_type

            if any([name_changed, country_changed, alpha_changed,
                    state_changed, type_changed]):
                update_query = '''
                UPDATE institutions
                SET name = %s,
                    country = %s,
                    alpha_two_code = %s,
                    state_province = %s,
                    type = %s
                WHERE name = %s
                '''
                update_params = (
                    institute['name'],
                    institute['country'],
                    institute['alpha_two_code'],
                    institute['state-province'],
                    insitute_type,
                    institute['name']
                )
                cur.execute(update_query, update_params)
                conn.commit()
                logging.info(f"Запись {institute['name']} изменена")
        else:
            query = '''
            INSERT INTO institutions (
                name,
                country,
                alpha_two_code,
                state_province,
                type)
            VALUES (%s, %s, %s, %s, %s)
            '''
            params = (institute['name'],
                      institute['country'],
                      institute['alpha_two_code'],
                      institute['state-province'],
                      insitute_type)
            cur.execute(query, params)
            conn.commit()
            logging.info(f"Добавлена новая запись {institute['name']}")

    cur.close()
    conn.close()


# def run_daily_job():
#     main()


# schedule.every().day.at("03:00").do(run_daily_job)

# if __name__ == "__main__":
#     main()

#     while True:
#         schedule.run_pending()
#         time.sleep(300)
