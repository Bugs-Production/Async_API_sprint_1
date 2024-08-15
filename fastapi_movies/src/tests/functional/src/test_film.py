import uuid

import pytest

from tests.functional.settings import test_settings

# fmt: off
test_data = [
    # 1 кейс, успешное получение фильмов
    {
        "page_size": 50,
        "page_num": 1,
        "result": 50,
        "status_code": 200
    },
    # 2 кейс, получение нужного размера страницы
    {
        "page_size": 20,
        "page_num": 1,
        "result": 20,
        "status_code": 200
    },
    # 3 кейс, успешное получение оставшихся фильмов по второй странице
    {
        "page_size": 50,
        "page_num": 2,
        "result": 10,
        "status_code": 200,
    },
    # 4 кейс, в случае когда переходим на номер страницы без фильмов, ожидаем 404
    {
        "page_size": 50,
        "page_num": 3,
        "result": 1,
        "status_code": 404,
    },
    # 5 кейс, успешное получение фильмов по id жанра
    {
        "page_size": 50,
        "page_num": 1,
        "genre": "fbd77e08-4dd6-4daf-9276-2abaa709fe87",
        "result": 50,
        "status_code": 200,
    },
    # 6 кейс, не нашли фильмы с нужным жанром, ожидаем 404
    {
        "page_size": 50,
        "page_num": 1,
        "genre": "6659b767-b656-49cf-80b2-6a7c012e9d22",
        "result": 1,
        "status_code": 404,
    },
]
# fmt: on


class TestFilmsApi:
    """Тестируем API для фильмов"""

    def setup_method(self):
        self.endpoint = "/api/v1/films"
        self.es_data = [
            {
                "id": str(uuid.uuid4()),
                "imdb_rating": 8.5,
                "genres": [
                    {"id": "6659b767-b656-49cf-80b2-6a7c012e9d21", "name": "Action"},
                    {"id": "fbd77e08-4dd6-4daf-9276-2abaa709fe87", "name": "Sci-Fi"},
                ],
                "title": "The Star",
                "description": "New World",
                "directors_names": ["Stan"],
                "actors_names": ["Ann", "Bob"],
                "writers_names": ["Ben", "Howard"],
                "directors": [{"id": str(uuid.uuid4()), "name": "Stan"}],
                "actors": [
                    {"id": str(uuid.uuid4()), "name": "Ann"},
                    {"id": str(uuid.uuid4()), "name": "Bob"},
                ],
                "writers": [
                    {"id": str(uuid.uuid4()), "name": "Ben"},
                    {"id": str(uuid.uuid4()), "name": "Howard"},
                ],
            }
            for _ in range(60)
        ]
        self.first_film_id = self.es_data[0]["id"]

    def preparation_bulk_query(self) -> list[dict]:
        bulk_query: list[dict] = []
        for row in self.es_data:
            data = {"_index": test_settings.es_index_movies, "_id": row["id"]}
            data.update({"_source": row})
            bulk_query.append(data)

        return bulk_query

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_case", test_data)
    async def test_all_films(self, aiohttp_request, es_write_data, test_case):
        await es_write_data(
            self.preparation_bulk_query(),
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
            self.preparation_bulk_query(),
            test_settings.es_index_movies,
            test_settings.es_mapping_films,
        )

        body, status = await aiohttp_request(
            method="GET",
            endpoint=f"{self.endpoint}/{self.first_film_id}",
        )

        assert status == 200
        assert len(body) == 8

    @pytest.mark.asyncio
    async def test_get_film_by_id_error(self, aiohttp_request, es_write_data):
        await es_write_data(
            self.preparation_bulk_query(),
            test_settings.es_index_movies,
            test_settings.es_mapping_films,
        )

        body, status = await aiohttp_request(
            method="GET",
            endpoint=f"{self.endpoint}/asd",
        )

        assert status == 404
        assert len(body) == 1
