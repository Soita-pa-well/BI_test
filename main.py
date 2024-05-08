import requests
import psycopg2
from airflow import DAG
from airflow.decorators import task


university_url = 'https://raw.githubusercontent.com/Hipo/university-domains-list/master/world_universities_and_domains.json'

response = requests.get(university_url)

data = response.json()

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="rfrfirf",
    host="localhost",
    port=5433,
)

cur = conn.cursor()

create_table_query = '''
CREATE TABLE institutions (
    id SERIAL PRIMARY KEY,
    name TEXT,
    domains TEXT[],
    web_pages TEXT[],
    country TEXT,
    alpha_two_code CHAR(2),
    state_province TEXT
);
'''

cur.execute(create_table_query)
conn.commit()
cur.close()
conn.close()
