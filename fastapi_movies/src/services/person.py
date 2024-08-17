from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Any

from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import ElasticStorage, get_elastic
from db.redis import PersonsRedisCache, get_redis
from models.models import PersonDetail

from .utils import get_offset_params, get_search_params


class AbstractPersonService(ABC):
    @abstractmethod
    async def get_by_id(self, person_id: Any) -> PersonDetail | None:
        pass

    @abstractmethod
    async def search(self, *args, **kwargs) -> list[PersonDetail]:
        pass


class PersonService(AbstractPersonService):
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = PersonsRedisCache(redis)
        self.elastic = ElasticStorage(elastic)
        self._index = "persons"

    async def get_by_id(self, person_id: str) -> PersonDetail | None:
        person = await self.redis.get_person(person_id)
        if person:
            return person

        doc = await self.elastic.get(index=self._index, id=person_id)
        if not doc:
            return None

        person = PersonDetail(**(doc["_source"]))

        await self.redis.put_person(person)

        return person

    async def search(
        self,
        query: str,
        page_num: int,
        page_size: int,
    ) -> list[PersonDetail] | None:
        search_params = get_search_params(field="full_name", query=query)
        offset_params = get_offset_params(page_num, page_size)
        params = {**search_params, **offset_params}

        persons = await self.redis.get_persons(query, page_num, page_size)
        if persons:
            return persons

        doc = await self.elastic.get_batch(index=self._index, body=params)
        if not doc:
            return None

        hits_persons = doc["hits"]["hits"]
        persons = [PersonDetail(**person["_source"]) for person in hits_persons]

        await self.redis.put_persons(persons, query, page_num, page_size)

        return persons


@lru_cache()
def get_person_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)
