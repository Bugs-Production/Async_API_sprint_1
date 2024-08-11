from functools import lru_cache

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import GenresRedisCache, get_redis
from models.models import GenreDetail

from .utils import get_offset_params


class GenreService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = GenresRedisCache(redis)
        self.elastic = elastic
        self._index = "genres"

    async def get_all_genres(
        self, page_num: int, page_size: int
    ) -> list[GenreDetail] | None:
        query = {"query": {"match_all": {}}}
        offset_params = get_offset_params(page_num, page_size)
        params = {**query, **offset_params}

        # пытаемся найти список жанров по номеру и размеру страницы
        genres_list = await self.redis.get_genres(page_num, page_size)
        if genres_list:
            return genres_list

        try:
            genres = await self.elastic.search(index=self._index, body=params)
        except NotFoundError:
            return None

        hits_genres = genres["hits"]["hits"]
        genres_list = [GenreDetail(**genre["_source"]) for genre in hits_genres]

        # сохраняем список жанров в кэш
        await self.redis.put_genres(
            genres_list,
            page_num,
            page_size,
        )

        return genres_list

    async def get_by_id(self, genre_id: str) -> GenreDetail | None:
        # Пытаемся получить данные из кеша, потому что оно работает быстрее
        genre = await self.redis.get_genre(genre_id=genre_id)
        if genre:
            return genre

        # Если жанра нет в кеше, то ищем его в Elasticsearch
        genre = await self._get_genre_from_elastic(genre_id)
        if not genre:
            # Если отсутствует в Elasticsearch - жанра вообще нет в базе
            return None

        await self.redis.put_genre(genre=genre)
        return genre

    async def _get_genre_from_elastic(self, genre_id: str) -> GenreDetail | None:
        try:
            doc = await self.elastic.get(index=self._index, id=genre_id)
        except NotFoundError:
            return None
        return GenreDetail(**doc["_source"])


@lru_cache()
def get_genre_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(redis, elastic)
