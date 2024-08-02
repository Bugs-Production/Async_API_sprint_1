import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_",
        env_file="/fastapi_movies/.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    db: str
    user: str
    password: str
    host: str
    port: str


class ElasticSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ELASTIC_",
        env_file="/fastapi_movies/.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = os.getenv("ELASTIC_HOST", "http://localhost:9200")
