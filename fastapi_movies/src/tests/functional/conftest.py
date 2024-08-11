import pytest
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from tests.functional.settings import test_settings


@pytest.fixture(name="es_write_data")
def es_write_data():
    async def inner(data: list[dict], index, mapping):
        es_client = AsyncElasticsearch(hosts=test_settings.es_host, verify_certs=False)
        if await es_client.indices.exists(index=index):
            await es_client.indices.delete(index=index)
        await es_client.indices.create(index=index, **mapping)

        updated, errors = await async_bulk(client=es_client, actions=data)

        await es_client.close()

        if errors:
            raise Exception("Ошибка записи данных в Elasticsearch")

    return inner
