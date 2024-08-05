import json
from functools import lru_cache

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
    ) -> list[GenreDetail] | None:
        query = {"query": {"match_all": {}}}
        offset_params = get_offset_params(page_num, page_size)
        params = {**query, **offset_params}

        # пытаемся найти список жанров по номеру и размеру страницы
        genres_list = await self._genre_or_genres_from_cache(
            genres_page_number=page_num,
            genres_page_size=page_size,
        )
        if genres_list:
            return genres_list

        try:
            genres = await self.elastic.search(index=self._index, body=params)
        except NotFoundError:
            return None

        hits_genres = genres["hits"]["hits"]
        genres_list = [GenreDetail(**genre["_source"]) for genre in hits_genres]

        # сохраняем список жанров в кэш
        await self._put_genre_or_genres_to_cache(
            genre_or_genres=genres_list,
            genres_page_number=page_num,
            genres_page_size=page_size,
        )

        return genres_list

    async def get_by_id(self, genre_id: str) -> GenreDetail | None:
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

    async def _get_genre_from_elastic(self, genre_id: str) -> GenreDetail | None:
        try:
            doc = await self.elastic.get(index=self._index, id=genre_id)
        except NotFoundError:
            return None
        return GenreDetail(**doc["_source"])

    async def _genre_or_genres_from_cache(
        self,
        genre_id: str | None = None,
        genres_page_number: int | None = None,
        genres_page_size: int | None = None,
    ) -> GenreDetail | None:
        # если есть номер страницы и ее размер, возвращаем список жанров
        if genres_page_number and genres_page_size:
            list_genres = await self.redis.get(
                f"genres_{str(genres_page_number)}_{str(genres_page_size)}"
            )
            if list_genres:
                genres_json = json.loads(list_genres)
                return [GenreDetail.parse_obj(genre) for genre in genres_json]
            return None

        # иначе возвращаем один жанр
        data = await self.redis.get(genre_id)
        if not data:
            return None

        return GenreDetail.parse_raw(data)

    async def _put_genre_or_genres_to_cache(
        self,
        genre_or_genres: GenreDetail | list[GenreDetail],
        genres_page_number: int | None = None,
        genres_page_size: int | None = None,
    ) -> None:
        # если есть номер страницы и размер, сохраняем список жанров
        if genres_page_number and genres_page_size:
            genres_json = json.dumps([genre.dict() for genre in genre_or_genres])
            await self.redis.set(
                f"genres_{str(genres_page_number)}_{str(genres_page_size)}",
                genres_json,
                CACHE_EXPIRE_IN_SECONDS,
            )
        else:
            # иначе сохраняем один жанр
            await self.redis.set(
                genre_or_genres.id,
                genre_or_genres.model_dump_json(),
                CACHE_EXPIRE_IN_SECONDS,
            )


@lru_cache()
def get_genre_service(
    redis: Redis = Depends(get_redis),
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(redis, elastic)
