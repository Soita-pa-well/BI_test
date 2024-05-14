import requests

from typing import Any, Dict, List


def get_info() -> List[Dict[str, Any]]:
    UNIVERSITIES_URL = 'http://universities.hipolabs.com/search'
    response = requests.get(UNIVERSITIES_URL)
    universities_data = response.json()
    return universities_data
