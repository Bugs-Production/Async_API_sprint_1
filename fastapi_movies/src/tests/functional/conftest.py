import asyncio
from typing import Any

import aiohttp
import pytest
import redis.asyncio as redis
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import BulkIndexError, async_bulk

from tests.functional.settings import test_settings


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def es_client():
    es_client = AsyncElasticsearch(hosts=test_settings.es_host, verify_certs=False)
    yield es_client
    await es_client.close()


@pytest.fixture(scope="session")
def es_write_data(es_client: AsyncElasticsearch):
    async def inner(data: list[dict], index: str, mapping: dict):
        if await es_client.indices.exists(index=index):
            await es_client.indices.delete(index=index)
        await es_client.indices.create(index=index, **mapping)

        try:
            updated, errors = await async_bulk(
                client=es_client, actions=data, refresh="wait_for"
            )
        except BulkIndexError as exc:
            errors = exc.errors

        if errors:
            raise Exception(f"Ошибка записи данных в Elasticsearch: {errors}")

    return inner


@pytest.fixture(scope="session")
async def aiohttp_session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest.fixture(scope="session")
def aiohttp_request(aiohttp_session: aiohttp.ClientSession):
    async def inner(
        method: str, endpoint: str, **kwargs: dict[str, Any]
    ) -> tuple[dict, int]:
        url = test_settings.service_url + endpoint
        async with aiohttp_session.request(
            method=method, url=url, **kwargs
        ) as response:
            body = await response.json()
            status = response.status
            return body, status

    return inner


@pytest.fixture(scope="session")
async def redis_client():
    pool = redis.ConnectionPool.from_url(
        f"redis://{test_settings.redis_host}:{test_settings.redis_port}"
    )
    client = redis.Redis.from_pool(pool)
    yield client
    await client.close()


@pytest.fixture(scope="function")
async def redis_flushall(redis_client):
    await redis_client.flushall()
