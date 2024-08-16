from functools import lru_cache

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import ElasticStorage, get_elastic
from db.redis import FilmRedisCache, get_redis
from models.models import Film

from .utils import (get_genre_filter_params, get_offset_params,
                    get_search_params, get_sort_params)


class FilmService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = FilmRedisCache(redis)
        self.elastic = ElasticStorage(elastic)
        self._index = "movies"

    async def get_by_id(self, id: str) -> Film | None:
        film = await self.redis.get_film(film_id=id)
        if film:
            return film

        doc = await self.elastic.get(index=self._index, id=id)
        if not doc:
            return None

        film = Film(**doc["_source"])
        await self.redis.put_film(film=film)

        return film

    async def get_all_films(
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

        doc = await self.elastic.search(index=self._index, body=params)
        if not doc:
            return None

        hits_films = doc["hits"]["hits"]

        films = [Film(**film["_source"]) for film in hits_films]

        await self.redis.put_films(films, page_num, page_size, genre_filter, sorting)

        return films

    async def search_films(
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

        doc = await self.elastic.search(index=self._index, body=params)
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
