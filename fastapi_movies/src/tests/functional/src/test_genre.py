from datetime import datetime

import pytest

from tests.functional.conftest import aiohttp_request, es_write_data
from tests.functional.settings import test_settings


@pytest.mark.parametrize(
    "genre_id, expected_answer",
    [
        ("1ff0d3aa-e4a9-4035-8c48-e48c5f7568e4", {"status": 200, "length": 5}),
        ("35b63763-5ee6-4ad3-8165-852231a09f7c", {"status": 200, "length": 5}),
        ("35b63763", {"status": 404, "length": 1}),
    ],
)
@pytest.mark.asyncio
async def test_genres(aiohttp_request, es_write_data, genre_id, expected_answer):
    es_data = [
        {
            "id": "1ff0d3aa-e4a9-4035-8c48-e48c5f7568e4",
            "name": "Action",
            "description": "",
            "created": datetime.now().isoformat(),
            "modified": datetime.now().isoformat(),
        },
        {
            "id": "35b63763-5ee6-4ad3-8165-852231a09f7c",
            "name": "Adventure",
            "description": "",
            "created": datetime.now().isoformat(),
            "modified": datetime.now().isoformat(),
        },
        {
            "id": "40e45abf-82fb-4ba9-9eac-9505fdb0869b",
            "name": "Fantasy",
            "description": "",
            "created": datetime.now().isoformat(),
            "modified": datetime.now().isoformat(),
        },
    ]
    bulk_query: list[dict] = []
    for row in es_data:
        data = {"_index": test_settings.es_index_genres, "_id": row["id"]}
        data.update({"_source": row})
        bulk_query.append(data)

    await es_write_data(
        bulk_query, test_settings.es_index_genres, test_settings.es_mapping_genres
    )

    body, status = await aiohttp_request(
        method="GET", endpoint=f"/api/v1/genres/{genre_id}"
    )

    assert status == expected_answer["status"]
    assert len(body) == expected_answer["length"]
