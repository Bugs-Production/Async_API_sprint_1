import psycopg
from elasticsearch import Elasticsearch, NotFoundError
from psycopg import ClientCursor
from psycopg.rows import dict_row
from pydantic import BaseModel

from config.config import ElasticSettings, PostgresSettings
from config.elastic_mapping import (FILMS_MAPPING, GENRES_MAPPING,
                                    PERSONS_MAPPING)
from dto.loaders import (FilmsElasticTransformer, FilmsPostgresExtractor,
                         GenresElasticTransformer, GenresPostgresExtractor,
                         LoadManager, PersonsElasticTransformer,
                         PersonsPostgresExtractor, Postgres, Task)
from state.json_storage import JsonStorage
from state.state import State
from utils.constants import (FILM_WORK_STATE_KEY, GENRE_STATE_KEY,
                             GENRES_INDEX, MOVIES_INDEX, PERSON_STATE_KEY,
                             PERSONS_INDEX)
from utils.decorators import backoff


class Index(BaseModel):
    index: str
    mapping: dict


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
    indexes = [
        Index(index=MOVIES_INDEX, mapping=FILMS_MAPPING),
        Index(index=GENRES_INDEX, mapping=GENRES_MAPPING),
        Index(index=PERSONS_INDEX, mapping=PERSONS_MAPPING)
    ]
    for index in indexes:
        try:
            elastic.indices.get(index=index.index)
        except NotFoundError:
            elastic.indices.create(index=index.index, body=index.mapping)

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
        genre_task = Task(
            state_key=GENRE_STATE_KEY,
            elastic_index=GENRES_INDEX,
            extractor=GenresPostgresExtractor,
            el_transformer=GenresElasticTransformer,
            sql_path="storage/postgresql/queries/load_genres.sql",
        )
        person_task = Task(
            state_key=PERSON_STATE_KEY,
            elastic_index=PERSONS_INDEX,
            extractor=PersonsPostgresExtractor,
            el_transformer=PersonsElasticTransformer,
            sql_path="storage/postgresql/queries/load_persons.sql"
        )
        manager.load_to_elastic(film_work_task)
        manager.load_to_elastic(genre_task)
        manager.load_to_elastic(person_task)


if __name__ == "__main__":
    main()
