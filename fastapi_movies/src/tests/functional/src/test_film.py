from http import HTTPStatus

import pytest

from tests.functional.settings import test_settings

from ..test_data.es_data import movies_data

# fmt: off
test_data = [
    # 1 кейс, успешное получение фильмов
    {
        "page_size": 50,
        "page_num": 1,
        "result": 50,
        "status_code": HTTPStatus.OK
    },
    # 2 кейс, получение нужного размера страницы
    {
        "page_size": 20,
        "page_num": 1,
        "result": 20,
        "status_code": HTTPStatus.OK
    },
    # 3 кейс, успешное получение оставшихся фильмов по второй странице
    {
        "page_size": 50,
        "page_num": 2,
        "result": 10,
        "status_code": HTTPStatus.OK,
    },
    # 4 кейс, в случае когда переходим на номер страницы без фильмов, ожидаем 404
    {
        "page_size": 50,
        "page_num": 3,
        "result": 1,
        "status_code": HTTPStatus.NOT_FOUND,
    },
    # 5 кейс, успешное получение фильмов по id жанра
    {
        "page_size": 50,
        "page_num": 1,
        "genre": "fbd77e08-4dd6-4daf-9276-2abaa709fe87",
        "result": 50,
        "status_code": HTTPStatus.OK,
    },
    # 6 кейс, не нашли фильмы с нужным жанром, ожидаем 404
    {
        "page_size": 50,
        "page_num": 1,
        "genre": "6659b767-b656-49cf-80b2-6a7c012e9d22",
        "result": 1,
        "status_code": HTTPStatus.NOT_FOUND,
    },
]
# fmt: on


class TestFilmsApi:
    """Тестируем API для фильмов"""

    def setup_method(self):
        self.endpoint = "/api/v1/films"
        self.es_data = movies_data
        self.first_film_id = self.es_data[0]["_source"]["id"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_case", test_data)
    async def test_all_films(self, aiohttp_request, es_write_data, test_case):
        await es_write_data(
            self.es_data,
            test_settings.es_index_movies,
            test_settings.es_mapping_films,
        )

        body, status = await aiohttp_request(
            method="GET",
            endpoint=self.endpoint,
            params={
                "page_size": test_case.get("page_size"),
                "page_number": test_case.get("page_num"),
                "genre": test_case.get("genre", ""),
            },
        )

        assert status == test_case.get("status_code")
        assert len(body) == test_case.get("result")

    @pytest.mark.asyncio
    async def test_get_film_by_id_success(self, aiohttp_request, es_write_data):
        await es_write_data(
            self.es_data,
            test_settings.es_index_movies,
            test_settings.es_mapping_films,
        )

        body, status = await aiohttp_request(
            method="GET",
            endpoint=f"{self.endpoint}/{self.first_film_id}",
        )

        assert status == HTTPStatus.OK
        assert len(body) == 8

    @pytest.mark.asyncio
    async def test_get_film_by_id_error(self, aiohttp_request, es_write_data):
        await es_write_data(
            self.es_data,
            test_settings.es_index_movies,
            test_settings.es_mapping_films,
        )

        body, status = await aiohttp_request(
            method="GET",
            endpoint=f"{self.endpoint}/asd",
        )

        assert status == HTTPStatus.NOT_FOUND
        assert len(body) == 1
