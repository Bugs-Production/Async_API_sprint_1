from datetime import datetime
from typing import Any, Dict, List
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


class ElasticObject(BaseModel):
    id: str
    name: str


class ElasticFilmWork(BaseModel):
    id: str
    imdb_rating: float | None = None
    genres: List[Dict[str, Any]] | None = None
    title: str
    description: str | None = None
    directors_names: List[str] | None = None
    actors_names: List[str] | None = None
    writers_names: List[str] | None = None
    directors: List[Dict[str, Any]] | None = None
    actors: List[Dict[str, Any]] | None = None
    writers: List[Dict[str, Any]] | None = None


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
    roles: List
    title: str
    rating: float | None = None


class PostgresPerson(BaseModel):
    id: UUID
    full_name: str
    modified: datetime
    films: List[dict] | None = None


class ElasticPerson(BaseModel):
    id: str
    full_name: str
    films: List[Dict[str, Any]] | None = None
