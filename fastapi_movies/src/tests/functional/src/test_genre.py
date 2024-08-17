from http import HTTPStatus

import pytest

from tests.functional.settings import test_settings
from tests.functional.test_data.es_data import genres_data


class TestGenresApi:

    def setup_method(self):
        self.endpoint = "/api/v1/genres"
        self.es_data = genres_data

    @pytest.mark.parametrize(
        "genre_id, expected_answer",
        [
            # тестируем, что возвращается существующий объект
            (
                "1ff0d3aa-e4a9-4035-8c48-e48c5f7568e4",
                {"status": HTTPStatus.OK, "length": 5},
            ),
            # тестируем, что корректно возвращается ответ, если объекта нет в базе
            ("35b63763", {"status": HTTPStatus.NOT_FOUND, "length": 1}),
        ],
    )
    @pytest.mark.asyncio
    async def test_genres(
        self, aiohttp_request, es_write_data, genre_id, expected_answer
    ):
        await es_write_data(
            self.es_data, test_settings.es_index_genres, test_settings.es_mapping_genres
        )

        body, status = await aiohttp_request(
            method="GET", endpoint=f"{self.endpoint}/{genre_id}"
        )

        assert status == expected_answer["status"]
        assert len(body) == expected_answer["length"]

    @pytest.mark.parametrize(
        "genres_pagination_data, expected_answer",
        [
            # проверяем, что возвращаются все объекты на странице
            (
                {"page_number": 1, "page_size": 50},
                {"status": HTTPStatus.OK, "length": 5},
            ),
            # проверяем, что приходит корректный ответ c несуществующей страницы
            (
                {"page_number": 2, "page_size": 5},
                {"status": HTTPStatus.NOT_FOUND, "length": 1},
            ),
            # проверяем, что остаток записей есть на следующей странице
            (
                {"page_number": 2, "page_size": 3},
                {"status": HTTPStatus.OK, "length": 2},
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_genres_paginated(
        self, aiohttp_request, es_write_data, genres_pagination_data, expected_answer
    ):
        await es_write_data(
            self.es_data, test_settings.es_index_genres, test_settings.es_mapping_genres
        )

        body, status = await aiohttp_request(
            method="GET", endpoint=self.endpoint, params=genres_pagination_data
        )
        assert status == expected_answer["status"]
        assert len(body) == expected_answer["length"]
