from datetime import datetime
from typing import Any
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
    genres: list[str] | None = None
    actors: list[str] | None = None
    directors: list[str] | None = None
    writers: list[str] | None = None


class ElasticObject(BaseModel):
    id: str
    name: str


class ElasticFilmWork(BaseModel):
    id: str
    imdb_rating: float | None = None
    genres: list[dict[str, Any]] | None = None
    title: str
    description: str | None = None
    directors_names: list[str] | None = None
    actors_names: list[str] | None = None
    writers_names: list[str] | None = None
    directors: list[dict[str, Any]] | None = None
    actors: list[dict[str, Any]] | None = None
    writers: list[dict[str, Any]] | None = None


class PostgresGenre(BaseModel):
    id: UUID
    name: str
    description: str | None = None
    created: datetime
    modified: datetime


class ElasticGenre(BaseModel):
    id: str
    name: str
    description: str | None = None
    created: str
    modified: str


class PostgresPersonFilmwork(BaseModel):
    id: UUID
    roles: list
    title: str
    rating: float | None = None


class PostgresPerson(BaseModel):
    id: UUID
    full_name: str
    modified: datetime
    films: list[dict] | None = None


class ElasticPerson(BaseModel):
    id: str
    full_name: str
    films: list[dict[str, Any]] | None = None
