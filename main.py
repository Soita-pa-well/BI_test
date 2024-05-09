import requests
import psycopg2
import os
from dotenv import load_dotenv
# from airflow import DAG
# from airflow.decorators import task


university_url = 'https://raw.githubusercontent.com/Hipo/university-domains-list/master/world_universities_and_domains.json'

response = requests.get(university_url)

data = response.json()

load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('PASSWORD'),
    host=os.getenv('HOST'),
    port=os.getenv('PORT'),
)

cur = conn.cursor()

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
    
    query = '''
    INSERT INTO institutions (name, country, alpha_two_code, state_province, type)
    VALUES (%s, %s, %s, %s, %s)
    '''

    params = (institute['name'], institute['country'],
              institute['alpha_two_code'], institute['state-province'],
              insitute_type)
    cur.execute(query, params)
    conn.commit()

cur.close()
conn.close()
