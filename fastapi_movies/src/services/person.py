from functools import lru_cache
from typing import Optional

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
        self._index = "persons_sprint4_test"

    async def get_by_id(self, person_id: str) -> Optional[PersonDetail]:
        person = await self._person_from_cache(person_id)
        if not person:
            person = await self._get_person_from_elastic(person_id)
            if not person:
                return None
            await self._put_person_to_cache(person)
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

        try:
            persons = await self.elastic.search(index=self._index, body=params)
        except NotFoundError:
            return None

        hits_persons = persons["hits"]["hits"]

        return [PersonDetail(**person["_source"]) for person in hits_persons]

    async def _get_person_from_elastic(self, person_id: str) -> Optional[PersonDetail]:
        try:
            doc = await self.elastic.get(index=self._index, id=person_id)
        except NotFoundError:
            return None
        return PersonDetail(**(doc["_source"]))

    async def _person_from_cache(self, person_id: str) -> Optional[PersonDetail]:
        data = await self.redis.get(person_id)
        if not data:
            return None

        person = PersonDetail.parse_raw(data)
        return person

    async def _put_person_to_cache(self, person: PersonDetail):
        await self.redis.set(person.id, person.json(), CACHE_EXPIRE_IN_SECONDS)


@lru_cache()
def get_person_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)
