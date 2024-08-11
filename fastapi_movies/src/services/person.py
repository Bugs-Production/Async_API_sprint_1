from functools import lru_cache

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import PersonsRedisCache, get_redis
from models.models import PersonDetail

from .utils import get_offset_params, get_search_params


class PersonService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = PersonsRedisCache(redis)
        self.elastic = elastic
        self._index = "persons"

    async def get_by_id(self, person_id: str) -> PersonDetail | None:
        person = await self.redis.get_person(person_id)
        if person:
            return person

        person = await self._get_person_from_elastic(person_id)
        if not person:
            return None
        await self.redis.put_person(person)
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
        persons_list = await self.redis.get_persons(
            query,
            page_num,
            page_size,
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
        await self.redis.put_persons(
            persons_list,
            query,
            page_num,
            page_size,
        )

        return persons_list

    async def _get_person_from_elastic(self, person_id: str) -> PersonDetail | None:
        try:
            doc = await self.elastic.get(index=self._index, id=person_id)
        except NotFoundError:
            return None
        return PersonDetail(**(doc["_source"]))


@lru_cache()
def get_person_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)
