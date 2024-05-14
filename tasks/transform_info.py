from typing import List

from airflow.models.taskinstance import TaskInstance

from university_models import University


def info_transformation(**kwargs) -> List[University]:
    ti: TaskInstance = kwargs['ti']
    data: list[dict] = ti.xcom_pull(task_ids='get_info_task')
    university_type = {
        'Institute': 'Institute',
        'College': 'College',
        'University': 'University'
    }
    universities_list = [
        University(
            country=item['country'],
            alpha_two_code=item['alpha_two_code'],
            state_province=item['state-province'],
            name=item['name'],
            type=next((university_type[i] for i in university_type
                      if i in item['name']), None)
        ) for item in data
    ]
    return universities_list
