from typing import Any

import aiohttp
import pytest
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from tests.functional.settings import test_settings


@pytest.fixture(name="es_write_data")
def es_write_data():
    async def inner(data: list[dict], index: str, mapping: dict):
        es_client = AsyncElasticsearch(hosts=test_settings.es_host, verify_certs=False)
        if await es_client.indices.exists(index=index):
            await es_client.indices.delete(index=index)
        await es_client.indices.create(index=index, **mapping)

        updated, errors = await async_bulk(client=es_client, actions=data)

        await es_client.close()

        if errors:
            raise Exception("Ошибка записи данных в Elasticsearch")

    return inner


@pytest.fixture(name="aiohttp_client_data")
def aiohttp_client_data():
    async def inner(
        method: str, endpoint: str, **kwargs: dict[str, Any]
    ) -> tuple[dict, int]:
        async with aiohttp.ClientSession() as session:
            url = test_settings.service_url + endpoint
            async with session.request(method=method, url=url, **kwargs) as response:
                body = await response.json()
                status = response.status
            return body, status

    return inner
