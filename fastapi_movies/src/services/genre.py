from functools import lru_cache
from typing import Optional

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import GenreDetail

from .utils import CACHE_EXPIRE_IN_SECONDS, get_offset_params


class GenreService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic
        self._index = "genres"

    async def get_all_genres(
            self, page_num: int, page_size: int
    ) -> list[GenreDetail]:
        query = {"query": {"match_all": {}}}
        offset_params = get_offset_params(page_num, page_size)
        params = {**query, **offset_params}

        genres = await self.elastic.search(index=self._index, body=params)

        hits_genres = genres["hits"]["hits"]

        return [GenreDetail(**genre["_source"]) for genre in hits_genres]

    async def get_by_id(self, genre_id: str) -> Optional[GenreDetail]:
        # Пытаемся получить данные из кеша, потому что оно работает быстрее
        genre = await self._genre_from_cache(genre_id)
        if not genre:
            # Если жанра нет в кеше, то ищем его в Elasticsearch
            genre = await self._get_genre_from_elastic(genre_id)
            if not genre:
                # Если отсутствует в Elasticsearch - жанра вообще нет в базе
                return None
            await self._put_genre_to_cache(genre)

        return genre

    async def _get_genre_from_elastic(
            self, genre_id: str
    ) -> Optional[GenreDetail]:
        try:
            doc = await self.elastic.get(index=self._index, id=genre_id)
        except NotFoundError:
            return None
        return GenreDetail(**doc["_source"])

    async def _genre_from_cache(self, genre_id: str) -> Optional[GenreDetail]:
        # Пытаемся получить данные о жанре из кеша, используя команду get
        # https://redis.io/commands/get/
        data = await self.redis.get(genre_id)
        if not data:
            return None

        genre = GenreDetail.parse_raw(data)
        return genre

    async def _put_genre_to_cache(self, genre: GenreDetail):
        # Сохраняем данные о фильме, используя команду set
        # Выставляем время жизни кеша — 5 минут
        # https://redis.io/commands/set/
        # pydantic позволяет сериализовать модель в json
        await self.redis.set(
            genre.id, genre.model_dump_json(), CACHE_EXPIRE_IN_SECONDS
        )


@lru_cache()
def get_genre_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(redis, elastic)
