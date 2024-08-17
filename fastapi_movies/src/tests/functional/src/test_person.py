from http import HTTPStatus

import pytest

from tests.functional.settings import test_settings
from tests.functional.test_data.es_data import persons_data


class TestPersonsApi:

    def setup_method(self):
        self.endpoint = "/api/v1/persons"
        self.es_data = persons_data
        self.second_person_id = self.es_data[1]["_id"]
        self.es_data[1]["_source"]["films"] = []

    @pytest.mark.parametrize(
        "person_id, expected_answer",
        [
            # тестируем, что возвращается существующий объект
            (
                persons_data[0]["_id"],
                {"status": HTTPStatus.OK, "length": 3},
            ),
            # тестируем, что корректно возвращается ответ, если объекта нет в базе
            ("35b63763", {"status": HTTPStatus.NOT_FOUND, "length": 1}),
        ],
    )
    @pytest.mark.asyncio
    async def test_persons(
        self, aiohttp_request, es_write_data, person_id, expected_answer
    ):
        await es_write_data(
            self.es_data,
            test_settings.es_index_persons,
            test_settings.es_mapping_persons,
        )

        body, status = await aiohttp_request(
            method="GET", endpoint=f"{self.endpoint}/{person_id}"
        )

        assert status == expected_answer["status"]
        assert len(body) == expected_answer["length"]

    @pytest.mark.parametrize(
        "person_id, expected_answer",
        [
            # тестируем, что возвращается существующий объект
            (
                persons_data[0]["_id"],
                {"status": HTTPStatus.OK, "length": 2},
            ),
            # тестируем, что от персоны без фильмов возвращается корректный ответ
            (persons_data[1]["_id"], {"status": HTTPStatus.NOT_FOUND, "length": 1}),
            # тестируем, что корректно возвращается ответ, если объекта нет в базе
            ("35b63763", {"status": HTTPStatus.NOT_FOUND, "length": 1}),
        ],
    )
    @pytest.mark.asyncio
    async def test_persons(
        self, aiohttp_request, es_write_data, person_id, expected_answer
    ):
        await es_write_data(
            self.es_data,
            test_settings.es_index_persons,
            test_settings.es_mapping_persons,
        )

        body, status = await aiohttp_request(
            method="GET", endpoint=f"{self.endpoint}/{person_id}/film"
        )

        assert status == expected_answer["status"]
        assert len(body) == expected_answer["length"]
