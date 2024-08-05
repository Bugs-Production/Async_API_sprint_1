from datetime import datetime

from dto.models import ElasticObject


def get_time(timestring):
    time = datetime.fromisoformat(timestring)
    return time


def create_elastic_objects_list(
    objects_list: list[str],
) -> list[ElasticObject] | None:
    """
    Приводит данные из постгреса в формате ["id: name", ...] к списку объектов
    эластика
    :param objects_list: список строк ["id: name", ...]
    :return: список объектов ElasticObject
    """

    return (
        [
            ElasticObject(id=_object.split(": ")[0], name=_object.split(": ")[1])
            for _object in objects_list
        ]
        if objects_list is not None
        else []
    )
