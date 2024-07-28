from datetime import datetime
from typing import List
from uuid import UUID

from pydantic import BaseModel


class PostgresFilmWork(BaseModel):
    id: UUID
    title: str
    description: str | None = None
    rating: float | None = None
    type: str
    created: datetime
    modified: datetime
    genres: List[str] | None = None
    actors: List[str] | None = None
    directors: List[str] | None = None
    writers: List[str] | None = None


class ElasticPerson(BaseModel):
    id: UUID
    name: str


class ElasticFilmWork(BaseModel):
    id: str
    imdb_rating: float | None = None
    genres: List[str]
    title: str
    description: str | None = None
    directors_names: List[str] | None = None
    actors_names: List[str] | None = None
    writers_names: List[str] | None = None
    directors: List[ElasticPerson] | None = None
    actors: List[ElasticPerson] | None = None
    writers: List[ElasticPerson] | None = None
