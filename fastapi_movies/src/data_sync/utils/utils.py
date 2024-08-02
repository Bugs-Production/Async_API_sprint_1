from datetime import datetime
from typing import List, Optional

from dto.models import ElasticObject


def get_time(timestring):
    time = datetime.fromisoformat(timestring)
    return time


def create_elastic_objects_list(objects_list) -> Optional[List[ElasticObject]]:

    return (
        [
            ElasticObject(id=_object.split(": ")[0], name=_object.split(": ")[1])
            for _object in objects_list
        ]
        if objects_list is not None
        else []
    )
