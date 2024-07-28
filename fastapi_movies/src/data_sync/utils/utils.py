from datetime import datetime
from typing import List, Optional

from data_sync.dto.models import ElasticPerson


def get_time(timestring):
    time = datetime.fromisoformat(timestring)
    return time


def create_elastic_persons_list(persons_list) -> Optional[List[ElasticPerson]]:

    return (
        [
            ElasticPerson(id=person.split(": ")[0], name=person.split(": ")[1])
            for person in persons_list
        ]
        if persons_list is not None
        else []
    )
