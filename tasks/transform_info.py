from get_info_task import get_info
from university_models import University

data = get_info()

university_type = {
    'Institute': 'Institute',
    'College': 'College',
    'University': 'University'
}

# universities = [University(country=item['country'],
#                            alpha_two_code=item['alpha_two_code'],
#                            state_province=item['state-province'],
#                            name=item['name'],
#                            type=[i for i in university_type if i in item['name'] else None]
#                            ) for item in data]
universities = [
    University(
        country=item['country'],
        alpha_two_code=item['alpha_two_code'],
        state_province=item['state-province'],
        name=item['name'],
        type=next((university_type[i] for i in university_type if i in item['name']), 'None')
    ) for item in data
]
print(universities)
