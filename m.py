from api import get_info
from db import connect_to_database, check_table_existence, create_table
# from airflow import DAG
# from airflow.decorators import task

conn = connect_to_database()
cur = conn.cursor()

if not check_table_existence(cur):
    create_table(cur, conn)

university_url = (
    'https://raw.githubusercontent.com/Hipo/'
    'university-domains-list/master/world_universities_and_domains.json'
)

data = get_info(university_url)
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
                state_changed]):
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

cur.close()
conn.close()

# if __name__ == "__main__":
#     main()
