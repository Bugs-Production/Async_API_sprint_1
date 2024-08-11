import json
from functools import lru_cache

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import FilmRedisCache, get_redis
from models.models import Film

from .utils import (CACHE_EXPIRE_IN_SECONDS, create_cache_key_for_films,
                    get_genre_filter_params, get_offset_params,
                    get_search_params, get_sort_params)


class FilmService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = FilmRedisCache(redis)
        self.elastic = elastic
        self._index = "movies"
        self.redis_cache = FilmRedisCache

    async def get_by_id(self, film_id: str) -> Film | None:
        # если находим фильм в кэше, достаем от туда
        film = await self.redis.get_film(film_id=film_id)
        if film:
            return film

        # Если фильма нет в кеше, то ищем его в Elasticsearch
        film = await self._get_film_from_elastic(film_id)
        if not film:
            # Если он отсутствует в Elasticsearch, значит, фильма вообще нет в базе
            return None
        # Сохраняем фильм в кеш
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

        # пытаемся найти фильмы в кэше
        list_films = await self.redis.get_films(
            page_num,
            page_size,
            sorting,
            genre_filter,
        )
        if list_films:
            return list_films

        # если в кэше нет, идем в эластик
        try:
            films = await self.elastic.search(index=self._index, body=params)
        except NotFoundError:
            return None

        hits_films = films["hits"]["hits"]

        list_films = [Film(**film["_source"]) for film in hits_films]

        # сохраняем в кэш по параметрам
        await self.redis.put_films(
            list_films,
            page_num,
            page_size,
            genre_filter,
            sorting,
        )

        return list_films

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

        # пытаемся найти фильмы в кэше
        list_films = await self.redis.get_films(
            page_num,
            page_size,
            sorting,
            query,
        )
        if list_films:
            return list_films

        try:
            films = await self.elastic.search(index=self._index, body=params)
        except NotFoundError:
            return None

        hits_films = films["hits"]["hits"]

        list_films = [Film(**film["_source"]) for film in hits_films]

        # сохраняем в кэш по параметрам
        await self.redis.put_films(
            list_films,
            page_num,
            page_size,
            query,
            sorting,
        )

        return list_films

    async def _get_film_from_elastic(self, film_id: str) -> Film | None:
        try:
            doc = await self.elastic.get(index=self._index, id=film_id)
        except NotFoundError:
            return None
        return Film(**doc["_source"])


@lru_cache()
def get_film_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)
