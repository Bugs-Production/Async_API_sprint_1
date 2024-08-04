import json
from functools import lru_cache
from typing import List, Optional, Union

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import PersonDetail

from .utils import (CACHE_EXPIRE_IN_SECONDS, get_offset_params,
                    get_search_params)


class PersonService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic
        self._index = "persons"

    async def get_by_id(self, person_id: str) -> Optional[PersonDetail]:
        person = await self._person_or_persons_from_cache(person_id)
        if not person:
            person = await self._get_person_from_elastic(person_id)
            if not person:
                return None
            await self._put_persons_or_person_to_cache(person)
        return person

    async def search_persons(
        self,
        query: str,
        page_num: int,
        page_size: int,
    ) -> list[PersonDetail] | None:
        search_params = get_search_params(field="full_name", query=query)
        offset_params = get_offset_params(page_num, page_size)
        params = {**search_params, **offset_params}

        # находим личностей в кэше
        persons_list = await self._person_or_persons_from_cache(
            person_search=query,
            persons_page_number=page_num,
            persons_page_size=page_size,
        )
        if persons_list:
            return persons_list

        try:
            persons = await self.elastic.search(index=self._index, body=params)
        except NotFoundError:
            return None

        hits_persons = persons["hits"]["hits"]
        persons_list = [PersonDetail(**person["_source"]) for person in hits_persons]

        # сохраняем в кэш
        await self._put_persons_or_person_to_cache(
            persons_or_person=persons_list,
            persons_search=query,
            persons_page_number=page_num,
            persons_page_size=page_size,
        )

        return persons_list

    async def _get_person_from_elastic(self, person_id: str) -> Optional[PersonDetail]:
        try:
            doc = await self.elastic.get(index=self._index, id=person_id)
        except NotFoundError:
            return None
        return PersonDetail(**(doc["_source"]))

    async def _person_or_persons_from_cache(
        self,
        person_id: Optional[str] = None,
        person_search: Optional[str] = None,
        persons_page_number: Optional[int] = None,
        persons_page_size: Optional[int] = None,
    ) -> Optional[Union[PersonDetail, List[PersonDetail], None]]:
        # если есть поиск по полю, отдаем список личностей
        if person_search:
            list_persons = await self.redis.get(
                f"persons_{person_search}_{str(persons_page_number)}_{str(persons_page_size)}"
            )
            if list_persons:
                persons_json = json.loads(list_persons)
                return [PersonDetail.parse_obj(person) for person in persons_json]
            return None

        # иначе возвращаем одну личность
        data = await self.redis.get(person_id)
        if not data:
            return None

        return PersonDetail.parse_raw(data)

    async def _put_persons_or_person_to_cache(
        self,
        persons_or_person: Union[PersonDetail, List[PersonDetail]],
        persons_search: Union[str] = None,
        persons_page_number: Optional[int] = None,
        persons_page_size: Optional[int] = None,
    ) -> None:
        # если есть поиск по полю, от отдаем список личностей
        if persons_search:
            persons_json = json.dumps([person.dict() for person in persons_or_person])
            await self.redis.set(
                f"persons_{persons_search}_{str(persons_page_number)}_{str(persons_page_size)}",
                persons_json,
                CACHE_EXPIRE_IN_SECONDS,
            )
        else:
            await self.redis.set(
                persons_or_person.id, persons_or_person.json(), CACHE_EXPIRE_IN_SECONDS
            )


@lru_cache()
def get_person_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)
