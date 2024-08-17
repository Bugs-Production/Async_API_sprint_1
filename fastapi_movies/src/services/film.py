from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Any

from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import ElasticStorage, get_elastic
from db.redis import FilmRedisCache, get_redis
from models.models import Film

from .utils import (get_genre_filter_params, get_offset_params,
                    get_search_params, get_sort_params)


class AbstractFilmService(ABC):
    @abstractmethod
    async def get_by_id(self, film_id: Any) -> Film | None:
        pass

    @abstractmethod
    async def get_all(self, *args, **kwargs) -> list[Film] | None:
        pass

    @abstractmethod
    async def search(self, *args, **kwargs) -> list[Film] | None:
        pass


class FilmService(AbstractFilmService):
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = FilmRedisCache(redis)
        self.elastic = ElasticStorage(elastic)
        self._index = "movies"

    async def get_by_id(self, film_id: str) -> Film | None:
        film = await self.redis.get_film(film_id=film_id)
        if film:
            return film

        doc = await self.elastic.get(index=self._index, id=film_id)
        if not doc:
            return None

        film = Film(**doc["_source"])
        await self.redis.put_film(film=film)

        return film

    async def get_all(
        self,
        sorting: str,
        genre_filter: str | None,
        page_num: int,
        page_size: int,
    ) -> list[Film] | None:
        sort_params = get_sort_params(sorting)
        genre_params = get_genre_filter_params(genre_filter)
        offset_params = get_offset_params(page_num, page_size)
        params = {**sort_params, **genre_params, **offset_params}

        films = await self.redis.get_films(page_num, page_size, sorting, genre_filter)
        if films:
            return films

        doc = await self.elastic.get_batch(index=self._index, body=params)
        if not doc:
            return None

        hits_films = doc["hits"]["hits"]

        films = [Film(**film["_source"]) for film in hits_films]

        await self.redis.put_films(films, page_num, page_size, genre_filter, sorting)

        return films

    async def search(
        self,
        sorting: str,
        query: str,
        page_num: int,
        page_size: int,
    ) -> list[Film] | None:
        sort_params = get_sort_params(sorting)
        search_params = get_search_params(field="title", query=query)
        offset_params = get_offset_params(page_num, page_size)
        params = {**sort_params, **search_params, **offset_params}

        films = await self.redis.get_films(page_num, page_size, sorting, query)
        if films:
            return films

        doc = await self.elastic.get_batch(index=self._index, body=params)
        if not doc:
            return None

        hits_films = doc["hits"]["hits"]

        films = [Film(**film["_source"]) for film in hits_films]

        await self.redis.put_films(films, page_num, page_size, query, sorting)

        return films


@lru_cache()
def get_film_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)
