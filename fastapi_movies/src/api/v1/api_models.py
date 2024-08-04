from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class IdMixIn(BaseModel):
    id: UUID = Field(default_factory=uuid4, serialization_alias="uuid")


class Genre(IdMixIn):
    name: str


class Person(IdMixIn):
    full_name: str = Field(validation_alias="name")


class PersonFilm(IdMixIn):
    roles: list[str]


class PersonDetail(IdMixIn):
    full_name: str
    films: list[PersonFilm] | None = None


class Film(IdMixIn):
    title: str
    imdb_rating: float | None


class FilmDetail(IdMixIn):
    title: str
    imdb_rating: float | None
    description: str | None
    genre: list[Genre] = Field(validation_alias="genres")
    actors: list[Person]
    writers: list[Person]
    directors: list[Person]


class GenreDetail(IdMixIn):
    name: str
    description: str | None = None
    created: str
    modified: str
