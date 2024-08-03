from elasticsearch import AsyncElasticsearch, NotFoundError
from redis.asyncio import Redis
from models.models import Genre
from fastapi import Depends
from db.elastic import get_elastic
from db.redis import get_redis
from functools import lru_cache
from .utils import get_search_params, get_offset_params

GENRE_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class GenreService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic
        self._index = "genres"

    async def get_all_genres(self, page_num: int,
                             page_size: int):
        query = {'query': {'match_all': {}}}
        offset_params = get_offset_params(page_num, page_size)
        params = {**query, **offset_params}

        genres = await self.elastic.search(index=self._index, body=params)

        hits_genres = genres["hits"]["hits"]

        return [Genre(**genre["_source"]) for genre in hits_genres]


@lru_cache()
def get_genre_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(redis, elastic)
