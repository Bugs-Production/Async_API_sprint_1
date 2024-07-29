import logging
from datetime import datetime
from typing import Any, Dict, List

import psycopg
from elasticsearch import Elasticsearch, NotFoundError
from psycopg import ClientCursor
from psycopg.rows import dict_row

from data_sync.config.config import ElasticSettings, PostgresSettings
from data_sync.utils.constants import (ETL_FILMS_MAPPING, FILM_WORK_STATE_KEY,
                                       GENRE_STATE_KEY, MOVIES_INDEX,
                                       PG_FETCH_SIZE)
from data_sync.utils.decorators import backoff
from dto.loaders import (ElasticLoader, FilmsElasticTransformer,
                         FilmsPostgresExtractor)
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


def load_films(pg_conn: psycopg.Connection, state: State, elastic: Elasticsearch):
    pg = Postgres(
        conn=pg_conn,
        sql_path="storage/postgresql/queries/load_films.sql"
    )
    last_modified_film = state.get_state(FILM_WORK_STATE_KEY, datetime.min)
    while film_works_data := pg.execute(params={"dttm": last_modified_film}):
        elastic_films = []
        tmp_last_film_modified = last_modified_film
        for fm in film_works_data:
            pg_film_work = FilmsPostgresExtractor.extract_films(fm)
            elastic_film_work = FilmsElasticTransformer.transform(pg_film_work)
            elastic_films.append(elastic_film_work)
            tmp_last_film_modified = pg_film_work.modified
        ElasticLoader.load(elastic, MOVIES_INDEX, elastic_films)
        last_modified_film = tmp_last_film_modified
        state.save_state(FILM_WORK_STATE_KEY, str(last_modified_film))


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
    elastic.indices.delete(index=MOVIES_INDEX)
    try:
        elastic.indices.get(index=MOVIES_INDEX)
    except NotFoundError:
        elastic.indices.create(index=MOVIES_INDEX, body=ETL_FILMS_MAPPING)

    state = State(storage=JsonStorage())

    with psycopg.connect(
        **dsl, row_factory=dict_row, cursor_factory=ClientCursor
    ) as pg_conn:
        load_films(pg_conn, state, elastic)


if __name__ == "__main__":
    main()
