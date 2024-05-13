import requests


def get_info():
    UNIVERSITIES_URL = 'http://universities.hipolabs.com/search'
    response = requests.get(UNIVERSITIES_URL)
    universities_data = response.json()
    return universities_data
