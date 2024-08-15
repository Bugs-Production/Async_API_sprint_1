from http import HTTPStatus

import pytest

from ..settings import test_settings
from ..test_data.es_data import movies_data, persons_data


@pytest.mark.parametrize(
    "query_data, expected_answer",
    [
        (
            {"query": "The Star"},
            {"status": HTTPStatus.OK, "length": 50},
        ),
        (
            {"query": "Mashed potato"},
            {
                "status": HTTPStatus.NOT_FOUND,
                "length": 1,
            },
        ),
    ],
)
@pytest.mark.asyncio
async def test_search_films(
    query_data,
    expected_answer,
    es_write_data,
    aiohttp_request,
    redis_flushall,
):
    await es_write_data(
        data=movies_data,
        index=test_settings.es_index_movies,
        mapping=test_settings.es_mapping_films,
    )

    endpoint = "/api/v1/films/search"
    body, status = await aiohttp_request(
        method="get", endpoint=endpoint, params=query_data
    )

    assert status == expected_answer["status"]
    assert len(body) == expected_answer["length"]


@pytest.mark.asyncio
async def test_search_films_from_cache(
    aiohttp_request,
):
    endpoint = "/api/v1/films/search"
    body, status = await aiohttp_request(
        method="get", endpoint=endpoint, params={"query": "The Star"}
    )

    assert status == HTTPStatus.OK
    assert len(body) == 50


@pytest.mark.asyncio
async def test_search_persons(es_write_data, aiohttp_request, redis_flushall):
    await es_write_data(
        data=persons_data,
        index=test_settings.es_index_persons,
        mapping=test_settings.es_mapping_persons,
    )

    endpoint = "/api/v1/persons/search"
    body, status = await aiohttp_request(
        method="get", endpoint=endpoint, params={"query": "Lucas"}
    )

    assert status == HTTPStatus.OK
    assert len(body) == 50


@pytest.mark.parametrize(
    "persons_query_data, persons_expected_answer",
    [
        (
            {"query": "Lucas", "page_number": 1, "page_size": 5},
            {"status": HTTPStatus.OK, "length": 5},
        ),
        (
            {"query": "Lucas", "page_number": 2, "page_size": 60},
            {"status": HTTPStatus.NOT_FOUND, "length": 1},
        ),
    ],
)
@pytest.mark.asyncio
async def test_search_persons_paginated(
    es_write_data, aiohttp_request, persons_query_data, persons_expected_answer
):
    await es_write_data(
        data=persons_data,
        index=test_settings.es_index_persons,
        mapping=test_settings.es_mapping_persons,
    )

    endpoint = "/api/v1/persons/search"
    body, status = await aiohttp_request(
        method="get", endpoint=endpoint, params=persons_query_data
    )
    assert status == persons_expected_answer["status"]
    assert len(body) == persons_expected_answer["length"]
