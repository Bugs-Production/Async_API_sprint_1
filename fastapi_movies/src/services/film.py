from functools import lru_cache
from typing import Any, Optional

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.film import Film

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

    async def get_all_films(self, sorting: str, genre_filter: str | None) -> list[Film] | None:
        sort_params = self._get_sort_params(sorting)
        genre_params = self._get_filter_params(genre_filter)
        params = {**sort_params, **genre_params}

        films = await self.elastic.search(index=self._index, body=params)

        hits_films = films["hits"]["hits"]

        return [Film(**film["_source"]) for film in hits_films]

    async def search_films(self, sorting: str, query: str) -> list[Film] | None:
        sort_params = self._get_sort_params(sorting)
        search_params = self._get_search_params(query)
        params = {**sort_params, **search_params}

        films = await self.elastic.search(index=self._index, body=params)

        hits_films = films["hits"]["hits"]

        return [Film(**film['_source']) for film in hits_films]

    def _get_sort_params(self, sorting: str) -> dict[str, list[dict[str, str]]]:
        """ Параметры для запроса в Elastic с сортировкой по рейтингу"""
        return {
            "sort": [
                {"imdb_rating": "desc" if sorting.startswith("-") else "asc"}
            ],
        }

    def _get_filter_params(self, genre_filter: str | None) -> dict[str, Any]:
        """ Параметры для запроса в Elastic с фильтрацией по жанру"""
        genre_params = {"query": {}}
        if genre_filter:
            genre_params["query"] = {
                "nested": {
                    "path": "genres",
                    "query": {
                        "bool": {
                            "should": [
                                {"match": {"genres.id": genre_filter}}
                            ]
                        }
                    }
                }
            }
        else:
            genre_params["query"] = {"match_all": {}}

        return genre_params

    def _get_search_params(self, query: str) -> dict[str, Any]:
        """ Параметры для запроса в Elastic с простым поисковым запросом по названию фильма"""

        return {
            "query": {
              "match": {
                "title": {
                    "query": query,
                }
              }
            }
          }

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
