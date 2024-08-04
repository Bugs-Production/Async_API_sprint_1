import json
import logging
from functools import lru_cache
from typing import Optional, List, Union

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.models import GenreDetail

from .utils import CACHE_EXPIRE_IN_SECONDS, get_offset_params

logger = logging.getLogger(__name__)


class GenreService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic
        self._index = "genres"

    async def get_all_genres(self, page_num: int, page_size: int) -> Union[List[GenreDetail], None]:
        query = {"query": {"match_all": {}}}
        offset_params = get_offset_params(page_num, page_size)
        params = {**query, **offset_params}

        # пытаемся найти список жанров по номеру страницы
        genres_list = await self._genre_or_genres_from_cache(genres_page_number=page_num)
        if genres_list:
            return genres_list

        try:
            genres = await self.elastic.search(index=self._index, body=params)
        except NotFoundError:
            logger.info("ElasticSearch connect error")
            return None

        hits_genres = genres["hits"]["hits"]
        genres_list = [GenreDetail(**genre["_source"]) for genre in hits_genres]

        # сохраняем список жанров в кэш
        await self._put_genre_or_genres_to_cache(genre_or_genres=genres_list, genres_page_number=page_num)

        return genres_list

    async def get_by_id(self, genre_id: str) -> Optional[GenreDetail]:
        # Пытаемся получить данные из кеша, потому что оно работает быстрее
        genre = await self._genre_or_genres_from_cache(genre_id=genre_id)
        if genre:
            return genre

        # Если жанра нет в кеше, то ищем его в Elasticsearch
        genre = await self._get_genre_from_elastic(genre_id)
        if not genre:
            # Если отсутствует в Elasticsearch - жанра вообще нет в базе
            return None

        await self._put_genre_or_genres_to_cache(genre_or_genres=genre)
        return genre

    async def _get_genre_from_elastic(self, genre_id: str) -> Optional[GenreDetail]:
        try:
            doc = await self.elastic.get(index=self._index, id=genre_id)
        except NotFoundError:
            logger.info("ElasticSearch connect error")
            return None
        return GenreDetail(**doc["_source"])

    async def _genre_or_genres_from_cache(
            self,
            genre_id: Optional[str] = None,
            genres_page_number: Optional[int] = None,
    ) -> Optional[GenreDetail]:
        # если есть номер страницы, возвращаем список жанров
        if genres_page_number:
            list_genres = await self.redis.get(genres_page_number)
            if list_genres:
                genres_json = json.loads(list_genres)
                return [GenreDetail.parse_obj(genre) for genre in genres_json]
            return None

        # иначе возвращаем один жанр
        data = await self.redis.get(genre_id)
        if not data:
            return None

        genre = GenreDetail.parse_raw(data)
        return genre

    async def _put_genre_or_genres_to_cache(
            self,
            genre_or_genres: Union[GenreDetail, List[GenreDetail]],
            genres_page_number: Optional[int] = None,
    ) -> None:
        # если есть номер страницы сохраняем список жанров
        if genres_page_number:
            genres_json = json.dumps([genre.dict() for genre in genre_or_genres])
            await self.redis.set(f"genres_{str(genres_page_number)}", genres_json, CACHE_EXPIRE_IN_SECONDS)
        else:
            # иначе сохраняем один жанр
            await self.redis.set(genre_or_genres.id, genre_or_genres.model_dump_json(), CACHE_EXPIRE_IN_SECONDS)


@lru_cache()
def get_genre_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(redis, elastic)
