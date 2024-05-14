import logging

from connect_and_create_table import connect_and_create_table
from transform_info import info_transformation


logging.basicConfig(level=logging.INFO)


def load_info() -> None:
    data = info_transformation()
    conn = connect_and_create_table()
    cur = conn.cursor()

    for institute in data:
        cur.execute('''SELECT *
                    FROM institutions
                    WHERE name = %s''',
                    (institute.name,))
        existing_record = cur.fetchone()

        if existing_record:
            (id, existing_name, existing_country, existing_alpha,
             existing_state, existing_type) = existing_record

            # name_changed = existing_name = institute.name
            country_changed = existing_country != institute.country
            alpha_changed = existing_alpha != institute.alpha_two_code
            state_changed = existing_state != institute.state_province
            type_changed = existing_type != institute.type

            if any([country_changed, alpha_changed,
                    state_changed, type_changed]):
                update_query = '''
                UPDATE institutions
                SET
                    name = %s,
                    country = %s,
                    alpha_two_code = %s,
                    state_province = %s,
                    type = %s
                WHERE name = %s
                '''
                update_params = (
                    institute.name,
                    institute.country,
                    institute.alpha_two_code,
                    institute.state_province,
                    institute.type,
                    existing_name
                )
                cur.execute(update_query, update_params)
                conn.commit()
                logging.info(f"Запись {institute.name} обновлена")
            else:
                logging.info(f"Запись {institute.name}  не изменилась")
        else:
            insert_query = '''
            INSERT INTO institutions (
                name,
                country,
                alpha_two_code,
                state_province,
                type
            )
            VALUES (%s, %s, %s, %s, %s)
            '''
            insert_params = (
                institute.name,
                institute.country,
                institute.alpha_two_code,
                institute.state_province,
                institute.type
            )
            cur.execute(insert_query, insert_params)
            conn.commit()
            logging.info(f"Добавлена новая запись {institute.name}")

    logging.info('Таблица обновлена успешно')
    cur.close()
    conn.close()


load_info()
