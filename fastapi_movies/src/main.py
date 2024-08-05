from contextlib import asynccontextmanager

from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from redis.asyncio import Redis

from api.v1 import films, genres, persons
from core.config import settings as config
from db import elastic, redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    try:
        redis.redis = Redis(host=config.redis_host, port=config.redis_port)
        elastic.es = AsyncElasticsearch(hosts=[config.elastic_host])
        yield
    finally:
        # shutdown
        await redis.redis.close()
        await elastic.es.close()


app = FastAPI(
    lifespan=lifespan,
    title=config.project_name,
    docs_url="/api/openapi",
    openapi_url="/api/openapi.json",
    default_response_class=ORJSONResponse,
)


# Подключаем роутер к серверу, указав префикс /v1/films
# Теги указываем для удобства навигации по документации
app.include_router(films.router, prefix="/api/v1/films", tags=["films"])
app.include_router(persons.router, prefix="/api/v1/persons", tags=["persons"])
app.include_router(genres.router, prefix="/api/v1/genres", tags=["genres"])
