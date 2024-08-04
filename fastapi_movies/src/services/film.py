import json
from functools import lru_cache
from typing import List, Optional, Union

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import Film

from .utils import (CACHE_EXPIRE_IN_SECONDS, get_genre_filter_params,
                    get_offset_params, get_search_params, get_sort_params)


class FilmService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic
        self._index = "movies"

    async def get_by_id(self, film_id: str) -> Optional[Film]:
        # если находим фильм в кэше, достаем от туда
        film = await self._film_or_films_from_cache(film_id=film_id)
        if film:
            return film

        # Если фильма нет в кеше, то ищем его в Elasticsearch
        film = await self._get_film_from_elastic(film_id)
        if not film:
            # Если он отсутствует в Elasticsearch, значит, фильма вообще нет в базе
            return None
        # Сохраняем фильм в кеш
        await self._put_film_or_films_to_cache(films_or_film=film)
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
        list_films = await self._film_or_films_from_cache(films_page_num=page_num)
        if list_films:
            return list_films

        # если в кэше нет, идем в эластик
        films = await self.elastic.search(index=self._index, body=params)

        hits_films = films["hits"]["hits"]

        list_films = [Film(**film["_source"]) for film in hits_films]

        # сохраняем в кэш по номеру страницы
        await self._put_film_or_films_to_cache(
            films_or_film=list_films, films_page_num=page_num
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
        list_films = await self._film_or_films_from_cache(films_page_num=page_num)
        if list_films:
            return list_films

        films = await self.elastic.search(index=self._index, body=params)

        hits_films = films["hits"]["hits"]

        list_films = [Film(**film["_source"]) for film in hits_films]

        # сохраняем в кэш по номеру страницы
        await self._put_film_or_films_to_cache(
            films_page_num=page_num, films_or_film=list_films
        )

        return list_films

    async def _get_film_from_elastic(self, film_id: str) -> Optional[Film]:
        try:
            doc = await self.elastic.get(index=self._index, id=film_id)
        except NotFoundError:
            return None
        return Film(**doc["_source"])

    async def _film_or_films_from_cache(
        self, film_id=None, films_page_num=None
    ) -> Optional[List[Film]]:
        if films_page_num:  # если есть номер страницы, отдаем список фильмов по странице
            films_json = await self.redis.get(films_page_num)
            if films_json:
                films_data = json.loads(films_json)
                return [Film.parse_obj(film_data) for film_data in films_data]
            return None

        # иначе же находим один фильм по id
        data = await self.redis.get(film_id)

        # если в кэше нет такого фильма отдаем None
        if not data:
            return None

        # возвращаем фильм
        film = Film.parse_raw(data)
        return film

    async def _put_film_or_films_to_cache(
        self, films_or_film: Union[Film, List[Film]], films_page_num: Optional[int] = None
    ) -> None:
        # если есть номер страницы, сохраняем в кэш список фильмов
        if films_page_num:
            films_json = json.dumps([f.dict() for f in films_or_film])
            await self.redis.set(films_page_num, films_json, CACHE_EXPIRE_IN_SECONDS)
        else:
            # иначе сохраняем один фильм
            await self.redis.set(
                films_or_film.id, films_or_film.json(), CACHE_EXPIRE_IN_SECONDS
            )


@lru_cache()
def get_film_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)
