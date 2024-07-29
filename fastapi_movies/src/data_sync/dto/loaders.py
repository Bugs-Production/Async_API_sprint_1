import logging
from abc import ABC, abstractmethod
from typing import List

from elasticsearch import Elasticsearch
from pydantic import BaseModel

from data_sync.dto.models import ElasticFilmWork, PostgresFilmWork
from data_sync.utils.utils import create_elastic_objects_list

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresExtractor(ABC):
    @staticmethod
    @abstractmethod
    def extract_films(data: dict):
        """Валидирует данные, пришедшие из Postgres."""
        pass


class FilmsPostgresExtractor(PostgresExtractor):
    @staticmethod
    def extract_films(data: dict) -> PostgresFilmWork:
        film_work = PostgresFilmWork(
            id=data["id"],
            title=data["title"],
            description=data["description"],
            rating=data["rating"],
            type=data["type"],
            created=data["created"],
            modified=data["modified"],
            genres=data["genres"],
            actors=data["actors"],
            directors=data["directors"],
            writers=data["writers"],
        )
        return film_work


class ElasticTransformer(ABC):
    @staticmethod
    @abstractmethod
    def transform(data):
        pass


class FilmsElasticTransformer(ElasticTransformer):
    @staticmethod
    def transform(data: PostgresFilmWork) -> ElasticFilmWork:
        """Приводит данные из объекта PostgresFilmWork в формат
        для загрузки в Elastic.
        """
        el_actors = create_elastic_objects_list(data.actors)

        el_directors = create_elastic_objects_list(data.directors)

        el_writers = create_elastic_objects_list(data.writers)

        genres = create_elastic_objects_list(data.genres)

        film_work = ElasticFilmWork(
            id=str(data.id),
            imdb_rating=data.rating,
            genres=[genre.model_dump() for genre in genres],
            title=data.title,
            description=data.description,
            actors_names=[person.name for person in el_actors],
            directors_names=[person.name for person in el_directors],
            writers_names=[person.name for person in el_writers],
            actors=[person.model_dump() for person in el_actors],
            directors=[person.model_dump() for person in el_directors],
            writers=[person.model_dump() for person in el_writers],
        )

        return film_work


class ElasticLoader:
    @staticmethod
    def load(client: Elasticsearch, index, objects: List[BaseModel]) -> None:
        """Осуществляет балковую загрузку данных в Эластик."""
        body = []
        for _object in objects:
            body.append(
                {
                    "index": {
                        "_index": index,
                        "_id": _object.id,
                    }
                }
            )
            body.append({**_object.model_dump()})
        res = client.bulk(index=index, body=body)
        logger.info(res)
