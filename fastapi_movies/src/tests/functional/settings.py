from pydantic import Field
from pydantic_settings import BaseSettings


class TestSettings(BaseSettings):
    es_host: str = Field("http://127.0.0.1:9200", env="ELASTIC_HOST")
    es_index: str = ...
    es_id_field: str = ...
    es_index_mapping: dict = ...

    redis_host: str = Field("http://127.0.0.1", env="REDIS_HOST")
    redis_port: str = Field("6379", env="REDIS_PORT")
    service_url: str = Field("http://localhost")


test_settings = TestSettings()
