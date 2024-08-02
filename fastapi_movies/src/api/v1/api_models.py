from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class IdMixIn(BaseModel):
    id: UUID = Field(default_factory=uuid4)


class Genre(IdMixIn):
    name: str


class Person(IdMixIn):
    full_name: str = Field(alias="name")


class FilmShort(IdMixIn):
    title: str
    imdb_rating: float | None


class FilmDetails(IdMixIn):
    title: str
    imdb_rating: float | None
    description: str | None
    genre: list[Genre] = Field(alias="genres")
    actors: list[Person]
    writers: list[Person]
    directors: list[Person]
