import pytest_asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import BulkIndexError, async_bulk

from tests.functional.settings import test_settings


@pytest_asyncio.fixture(scope="session")
async def es_client():
    es_client = AsyncElasticsearch(hosts=test_settings.es_host, verify_certs=False)
    yield es_client
    await es_client.close()


@pytest_asyncio.fixture(scope="session")
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
