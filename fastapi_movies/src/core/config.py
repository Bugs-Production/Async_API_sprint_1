import os
from logging import config as logging_config

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from core.logger import LOGGING


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="/fastapi_movies/.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
    project_name: str = Field("movies", alias="PROJECT_NAME")
    redis_host: str = Field("127.0.0.1", alias="REDIS_HOST")
    redis_port: int = Field(6379, alias="REDIS_PORT")
    elastic_host: str = Field("127.0.0.1:9200", alias="ELASTIC_HOST")


settings = Settings()

# Применяем настройки логирования
logging_config.dictConfig(LOGGING)
