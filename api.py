import requests


def get_info(url):
    response = requests.get(url)
    data = response.json()
    return data