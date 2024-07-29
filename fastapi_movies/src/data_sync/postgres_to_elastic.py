import logging
from datetime import datetime
from typing import Any, Dict, List

import psycopg
from elasticsearch import Elasticsearch, NotFoundError
from psycopg import ClientCursor
from psycopg.rows import dict_row
from pydantic import BaseModel

from data_sync.config.config import ElasticSettings, PostgresSettings
from data_sync.utils.constants import (ETL_MAPPING, MOVIES_INDEX,
                                       PG_FETCH_SIZE, STATE_KEY)
from data_sync.utils.decorators import backoff
from data_sync.utils.utils import create_elastic_objects_list
from dto.models import ElasticFilmWork, PostgresFilmWork
from state.json_storage import JsonStorage
from state.state import State

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Postgres:
    def __init__(self, conn: psycopg.Connection, sql_path: str):
        self.conn = conn
        self.query = self.get_query(sql_path)

    def get_query(self, sql_path: str) -> bytes:
        with open(sql_path, "rb") as f:
            self.query = f.read()
            return self.query

    @backoff()
    def execute(self, params: Dict[str, Any]) -> List[dict]:
        with self.conn.cursor() as cursor:
            cursor.execute(self.query, params)
            rows = cursor.fetchmany(PG_FETCH_SIZE)
            return rows


class PostgresExtractor:
    @staticmethod
    def extract_films(film_work_data: dict) -> PostgresFilmWork:
        """Валидирует данные, пришедшие из Postgres."""
        film_work = PostgresFilmWork(
            id=film_work_data["id"],
            title=film_work_data["title"],
            description=film_work_data["description"],
            rating=film_work_data["rating"],
            type=film_work_data["type"],
            created=film_work_data["created"],
            modified=film_work_data["modified"],
            genres=film_work_data["genres"],
            actors=film_work_data["actors"],
            directors=film_work_data["directors"],
            writers=film_work_data["writers"],
        )
        return film_work


class ElasticTransformer:
    @staticmethod
    def transform_films(film_work_data: PostgresFilmWork) -> ElasticFilmWork:
        """Приводит данные из объекта PostgresFilmWork в формат
        для загрузки в Elastic.
        """
        el_actors = create_elastic_objects_list(film_work_data.actors)

        el_directors = create_elastic_objects_list(film_work_data.directors)

        el_writers = create_elastic_objects_list(film_work_data.writers)

        genres = create_elastic_objects_list(film_work_data.genres)

        film_work = ElasticFilmWork(
            id=str(film_work_data.id),
            imdb_rating=film_work_data.rating,
            genres=[genre.model_dump() for genre in genres],
            title=film_work_data.title,
            description=film_work_data.description,
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


@backoff()
def main():
    postgres_settings = PostgresSettings()
    elastic_settings = ElasticSettings()

    dsl = {
        "dbname": postgres_settings.db,
        "user": postgres_settings.user,
        "password": postgres_settings.password,
        "host": postgres_settings.host,
        "port": postgres_settings.port,
    }

    elastic = Elasticsearch(hosts=elastic_settings.host)
    try:
        elastic.indices.get(index=MOVIES_INDEX)
    except NotFoundError:
        elastic.indices.create(index=MOVIES_INDEX, body=ETL_MAPPING)

    state = State(storage=JsonStorage())

    with psycopg.connect(
        **dsl, row_factory=dict_row, cursor_factory=ClientCursor
    ) as pg_conn:
        pg = Postgres(
            conn=pg_conn, sql_path="storage/postgresql/queries/load_films.sql"
        )
        last_modified_film = state.get_state(STATE_KEY, datetime.min)
        while film_works_data := pg.execute(params={
            "dttm": last_modified_film
        }):
            elastic_films = []
            for fm in film_works_data:
                pg_film_work = PostgresExtractor.extract_films(fm)
                elastic_film_work = ElasticTransformer.transform_films(
                    pg_film_work
                )
                elastic_films.append(elastic_film_work)
                tmp_last_film_modified = pg_film_work.modified
            ElasticLoader.load(elastic, MOVIES_INDEX, elastic_films)
            last_modified_film = tmp_last_film_modified
            state.save_state(STATE_KEY, str(last_modified_film))


if __name__ == "__main__":
    main()
