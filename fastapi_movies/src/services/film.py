from functools import lru_cache
from typing import Optional

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import Film

from .utils import (get_genre_filter_params, get_offset_params,
                    get_search_params, get_sort_params)

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class FilmService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic
        self._index = "movies"

    # get_by_id возвращает объект фильма. Он опционален, так как фильм может отсутствовать в базе
    async def get_by_id(self, film_id: str) -> Optional[Film]:
        # Пытаемся получить данные из кеша, потому что оно работает быстрее
        film = await self._film_from_cache(film_id)
        if not film:
            # Если фильма нет в кеше, то ищем его в Elasticsearch
            film = await self._get_film_from_elastic(film_id)
            if not film:
                # Если он отсутствует в Elasticsearch, значит, фильма вообще нет в базе
                return None
            # Сохраняем фильм в кеш
            await self._put_film_to_cache(film)

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

        films = await self.elastic.search(index=self._index, body=params)

        hits_films = films["hits"]["hits"]

        return [Film(**film["_source"]) for film in hits_films]

    async def search_films(
        self,
        sorting: str,
        query: str,
        page_num: int,
        page_size: int,
    ) -> list[Film] | None:
        sort_params = get_sort_params(sorting)
        search_params = get_search_params(query)
        offset_params = get_offset_params(page_num, page_size)
        params = {**sort_params, **search_params, **offset_params}

        films = await self.elastic.search(index=self._index, body=params)

        hits_films = films["hits"]["hits"]

        return [Film(**film["_source"]) for film in hits_films]

    async def _get_film_from_elastic(self, film_id: str) -> Optional[Film]:
        try:
            doc = await self.elastic.get(index=self._index, id=film_id)
        except NotFoundError:
            return None
        return Film(**doc["_source"])

    async def _film_from_cache(self, film_id: str) -> Optional[Film]:
        # Пытаемся получить данные о фильме из кеша, используя команду get
        # https://redis.io/commands/get/
        data = await self.redis.get(film_id)
        if not data:
            return None

        # pydantic предоставляет удобное API для создания объекта моделей из json
        film = Film.parse_raw(data)
        return film

    async def _put_film_to_cache(self, film: Film):
        # Сохраняем данные о фильме, используя команду set
        # Выставляем время жизни кеша — 5 минут
        # https://redis.io/commands/set/
        # pydantic позволяет сериализовать модель в json
        await self.redis.set(film.id, film.json(), FILM_CACHE_EXPIRE_IN_SECONDS)


@lru_cache()
def get_film_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)
