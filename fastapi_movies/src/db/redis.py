import json
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Union

from redis.asyncio import Redis

from models.models import Film

redis: Redis | None = None


# Функция понадобится при внедрении зависимостей
async def get_redis() -> Redis:
    return redis


class AbstractRedisCache(ABC):
    """Абстрактный класс для кэширования Redis"""

    def __init__(self, redis_client):
        self.redis_client = redis_client

    @abstractmethod
    async def get_from_cache(self, key: str) -> Optional[Any]:
        pass

    @abstractmethod
    async def put_to_cache(self, key: str, value: Any, ttl: int) -> None:
        pass

    def create_cache_key(self, *args) -> str:
        return "_".join(str(arg) for arg in args if arg)


class RedisCache(AbstractRedisCache):
    """Реализуем интерфейс"""

    async def get_from_cache(self, key: str) -> Optional[Any]:
        data = await self.redis_client.get(key)
        if data:
            return json.loads(data)
        return None

    async def put_to_cache(self, key: str, value: Any, ttl: int) -> None:
        await self.redis_client.set(key, json.dumps(value), ex=ttl)


class FilmRedisCache(RedisCache):
    """Класс для кэширования фильмов"""

    CACHE_SECONDS_FOR_FILMS = 60 * 5

    async def get_film(self, film_id: str) -> Optional[Film]:
        data = await self.get_from_cache(film_id)
        if data:
            return Film.parse_obj(data)
        return None

    async def put_film(self, film: Film) -> None:
        await self.put_to_cache(film.id, film.dict(), self.CACHE_SECONDS_FOR_FILMS)

    async def get_films(self, *args) -> Optional[List[Film]]:
        cache_key = self.create_cache_key("films", *args)
        data = await self.get_from_cache(cache_key)
        if data:
            return [Film.parse_obj(item) for item in data]
        return None

    async def put_films(self, films: List[Film], *args) -> None:
        cache_key = self.create_cache_key("films", *args)
        await self.put_to_cache(
            cache_key, [film.dict() for film in films], self.CACHE_SECONDS_FOR_FILMS
        )
