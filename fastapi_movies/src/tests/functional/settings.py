from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from tests.functional.test_data.es_mapping import (FILMS_MAPPING,
                                                   GENRES_MAPPING,
                                                   PERSONS_MAPPING)


class TestSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="/fastapi_movies/.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    es_host: str = Field("http://127.0.0.1:9200", alias="ELASTIC_HOST")
    es_index_movies: str = Field("movies", alias="MOVIES_INDEX")
    es_index_genres: str = Field("genres", alias="GENRES_INDEX")
    es_index_persons: str = Field("persons", alias="PERSONS_INDEX")
    es_mapping_films: dict = FILMS_MAPPING
    es_mapping_genres: dict = GENRES_MAPPING
    es_mapping_persons: dict = PERSONS_MAPPING

    redis_host: str = Field("127.0.0.1", alias="REDIS_HOST")
    redis_port: str = Field("6379", alias="REDIS_PORT")
    service_url: str = Field("http://localhost:8080", alias="SERVICE_URL")


test_settings = TestSettings()
