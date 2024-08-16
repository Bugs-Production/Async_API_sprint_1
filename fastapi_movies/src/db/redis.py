import json
from typing import Any

from redis.asyncio import Redis

from db.base_models import AbstractCache
from models.models import Film, GenreDetail, PersonDetail

redis: Redis | None = None


# Функция понадобится при внедрении зависимостей
async def get_redis() -> Redis:
    return redis


class RedisCache(AbstractCache):
    """Реализуем интерфейс Redis"""

    CACHE_SECONDS = 60 * 5

    async def get_from_cache(self, key: str) -> dict | None:
        data = await self.cache_client.get(key)
        if data:
            return json.loads(data)
        return None

    async def put_to_cache(self, key: str, value: Any, ttl: int) -> None:
        await self.cache_client.set(key, json.dumps(value), ex=ttl)


class FilmRedisCache(RedisCache):
    """Класс для кэширования фильмов"""

    _cache_prefix = "films"

    async def get_film(self, film_id: str) -> Film | None:
        data = await self.get_from_cache(film_id)
        if data:
            return Film.parse_obj(data)
        return None

    async def put_film(self, film: Film) -> None:
        await self.put_to_cache(film.id, film.dict(), self.CACHE_SECONDS)

    async def get_films(self, *args) -> list[Film] | None:
        cache_key = self.create_cache_key(self._cache_prefix, *args)
        data = await self.get_from_cache(cache_key)
        if data:
            return [Film.parse_obj(item) for item in data]
        return None

    async def put_films(self, films: list[Film], *args) -> None:
        cache_key = self.create_cache_key(self._cache_prefix, *args)
        await self.put_to_cache(
            cache_key, [film.dict() for film in films], self.CACHE_SECONDS
        )


class GenresRedisCache(RedisCache):
    """Класс для кэширования жанров"""

    _cache_prefix = "genres"

    async def get_genre(self, genre_id: str) -> GenreDetail | None:
        data = await self.get_from_cache(genre_id)
        if data:
            return GenreDetail.parse_obj(data)
        return None

    async def put_genre(self, genre: GenreDetail) -> None:
        await self.put_to_cache(genre.id, genre.dict(), self.CACHE_SECONDS)

    async def get_genres(self, *args) -> list[GenreDetail] | None:
        cache_key = self.create_cache_key(self._cache_prefix, *args)
        data = await self.get_from_cache(cache_key)
        if data:
            return [GenreDetail.parse_obj(item) for item in data]
        return None

    async def put_genres(self, genres: list[GenreDetail], *args) -> None:
        cache_key = self.create_cache_key(self._cache_prefix, *args)
        await self.put_to_cache(
            cache_key, [genre.dict() for genre in genres], self.CACHE_SECONDS
        )


class PersonsRedisCache(RedisCache):
    """Класс для кэширования личностей"""

    _cache_prefix = "persons"

    async def get_person(self, person_id: str) -> PersonDetail | None:
        data = await self.get_from_cache(person_id)
        if data:
            return PersonDetail.parse_obj(data)
        return None

    async def put_person(self, person: PersonDetail) -> None:
        await self.put_to_cache(person.id, person.dict(), self.CACHE_SECONDS)

    async def get_persons(self, *args) -> list[PersonDetail] | None:
        cache_key = self.create_cache_key(self._cache_prefix, *args)
        data = await self.get_from_cache(cache_key)
        if data:
            return [PersonDetail.parse_obj(item) for item in data]
        return None

    async def put_persons(self, persons: list[GenreDetail], *args) -> None:
        cache_key = self.create_cache_key(self._cache_prefix, *args)
        await self.put_to_cache(
            cache_key,
            [person.dict() for person in persons],
            self.CACHE_SECONDS,
        )
