# Используем pydantic для упрощения работы при перегонке данных из json в объекты
# В этом файле описаны модели бизнес-логики
from pydantic import BaseModel


class IdMixIn(BaseModel):
    id: str


class Genre(IdMixIn):
    name: str
    description: str | None = None


class GenreDetail(IdMixIn):
    name: str
    description: str | None = None
    created: str
    modified: str


class PersonFilm(IdMixIn):
    roles: list[str]
    title: str
    imdb_rating: float | None


class PersonDetail(IdMixIn):
    full_name: str
    films: list[PersonFilm] | None = None


class Person(IdMixIn):
    name: str


class Film(IdMixIn):
    title: str
    description: str | None
    imdb_rating: float | None
    genres: list[Genre]
    actors: list[Person] | None
    writers: list[Person] | None
    directors: list[Person] | None
    actors_names: list[str] | None
    writers_names: list[str] | None
    directors_names: list[str] | None
