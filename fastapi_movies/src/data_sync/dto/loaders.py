import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List

import psycopg
from elasticsearch import Elasticsearch
from pydantic import BaseModel

from data_sync.dto.models import (
    ElasticFilmWork,
    ElasticGenre,
    PostgresFilmWork,
    PostgresGenre,
)
from data_sync.state.state import State
from data_sync.utils.constants import PG_FETCH_SIZE
from data_sync.utils.decorators import backoff
from data_sync.utils.utils import create_elastic_objects_list

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Postgres:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn

    @backoff()
    def execute(self, sql_path, params: Dict[str, Any]) -> List[dict]:
        with self.conn.cursor() as cursor:
            with open(sql_path, "rb") as f:
                query = f.read()
            cursor.execute(query, params)
            rows = cursor.fetchmany(PG_FETCH_SIZE)
            return rows


class PostgresExtractor(ABC):
    @staticmethod
    @abstractmethod
    def extract(data: dict):
        """Валидирует данные, пришедшие из Postgres."""
        pass


class FilmsPostgresExtractor(PostgresExtractor):
    @staticmethod
    def extract(data: dict) -> PostgresFilmWork:
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


class GenresPostgresExtractor(PostgresExtractor):
    @staticmethod
    def extract(data: dict) -> PostgresGenre:
        genre = PostgresGenre(
            id=data["id"],
            name=data["name"],
            description=data["description"],
            created=data["created"],
            modified=data["modified"]
        )
        return genre


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


class GenresElasticTransformer(ElasticTransformer):
    @staticmethod
    def transform(data: PostgresGenre) -> ElasticGenre:
        genre = ElasticGenre(
            id=str(data.id),
            name=data.name,
            description=data.description,
            created=data.created.isoformat(),
            modified=data.modified.isoformat()
        )
        return genre


class ElasticLoader:
    @staticmethod
    def load(client: Elasticsearch, index, objects: List[BaseModel]) -> Dict:
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
        return res


class Task:
    def __init__(
        self,
        state_key: str,
        elastic_index: str,
        extractor: PostgresExtractor,
        el_transformer: ElasticTransformer,
        sql_path: str,
    ):
        self.state_key = state_key
        self.elastic_index = elastic_index
        self.extractor = extractor
        self.el_transformer = el_transformer
        self.sql_path = sql_path


class LoadManager:
    def __init__(self, pg: Postgres, elastic: Elasticsearch, state: State):
        self.pg = pg
        self.elastic = elastic
        self.state = state

    def load_to_elastic(self, task: Task):
        last_modified_obj = self.state.get_state(task.state_key, datetime.min)
        while pg_data := self.pg.execute(
            sql_path=task.sql_path, params={"dttm": last_modified_obj}
        ):
            elastic_objects = []
            tmp_last_obj_modified = last_modified_obj
            for obj in pg_data:
                pg_obj = task.extractor.extract(obj)
                elastic_obj = task.el_transformer.transform(pg_obj)
                elastic_objects.append(elastic_obj)
                tmp_last_obj_modified = pg_obj.modified
            res = ElasticLoader.load(
                self.elastic, task.elastic_index, elastic_objects
            )
            if res.get('errors', True):
                logger.error('Elastic loader have a error!')
                break
            last_modified_obj = tmp_last_obj_modified
            self.state.save_state(task.state_key, str(last_modified_obj))
