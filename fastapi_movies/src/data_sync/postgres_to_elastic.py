import logging

import psycopg
from elasticsearch import Elasticsearch, NotFoundError
from psycopg import ClientCursor
from psycopg.rows import dict_row

from data_sync.config.config import ElasticSettings, PostgresSettings
from data_sync.utils.constants import (
    ETL_FILMS_MAPPING,
    FILM_WORK_STATE_KEY,
    MOVIES_INDEX,
)
from data_sync.utils.decorators import backoff
from dto.loaders import (
    FilmsElasticTransformer,
    FilmsPostgresExtractor,
    LoadManager,
    Postgres,
    Task,
)
from state.json_storage import JsonStorage
from state.state import State

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        elastic.indices.create(index=MOVIES_INDEX, body=ETL_FILMS_MAPPING)

    state = State(storage=JsonStorage())

    with psycopg.connect(
        **dsl, row_factory=dict_row, cursor_factory=ClientCursor
    ) as pg_conn:
        pg = Postgres(pg_conn)
        manager = LoadManager(pg=pg, elastic=elastic, state=state)
        film_work_task = Task(
            state_key=FILM_WORK_STATE_KEY,
            elastic_index=MOVIES_INDEX,
            extractor=FilmsPostgresExtractor,
            el_transformer=FilmsElasticTransformer,
            sql_path="storage/postgresql/queries/load_films.sql",
        )
        manager.load_to_elastic(film_work_task)


if __name__ == "__main__":
    main()
