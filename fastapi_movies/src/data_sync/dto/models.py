from datetime import datetime
from typing import List, Dict, Any
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
    id: UUID
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