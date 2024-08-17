from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Any

from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import ElasticStorage, get_elastic
from db.redis import GenresRedisCache, get_redis
from models.models import GenreDetail

from .utils import get_offset_params


class AbstractGenreService(ABC):
    @abstractmethod
    async def get_by_id(self, genre_id: Any) -> GenreDetail | None:
        pass

    @abstractmethod
    async def get_all(self, *args, **kwargs) -> list[GenreDetail] | None:
        pass


class GenreService(AbstractGenreService):
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = GenresRedisCache(redis)
        self.elastic = ElasticStorage(elastic)
        self._index = "genres"

    async def get_by_id(self, genre_id: str) -> GenreDetail | None:
        genre = await self.redis.get_genre(genre_id=genre_id)
        if genre:
            return genre

        doc = await self.elastic.get(index=self._index, id=genre_id)
        if not doc:
            return None

        genre = GenreDetail(**doc["_source"])

        await self.redis.put_genre(genre=genre)

        return genre

    async def get_all(self, page_num: int, page_size: int) -> list[GenreDetail] | None:
        query = {"query": {"match_all": {}}}
        offset_params = get_offset_params(page_num, page_size)
        params = {**query, **offset_params}

        genres = await self.redis.get_genres(page_num, page_size)
        if genres:
            return genres

        doc = await self.elastic.get_batch(index=self._index, body=params)
        if not doc:
            return None

        hits_genres = doc["hits"]["hits"]
        genres = [GenreDetail(**genre["_source"]) for genre in hits_genres]

        await self.redis.put_genres(genres, page_num, page_size)

        return genres


@lru_cache()
def get_genre_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(redis, elastic)
