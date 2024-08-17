from elasticsearch import AsyncElasticsearch, NotFoundError

from db.base_models import AbstractStorage

es: AsyncElasticsearch | None = None


# Функция понадобится при внедрении зависимостей
async def get_elastic() -> AsyncElasticsearch:
    return es


class ElasticStorage(AbstractStorage):
    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def get(self, index: str, id: str) -> dict | None:
        try:
            doc = await self.elastic.get(index=index, id=id)
        except NotFoundError:
            return None
        return doc

    async def get_batch(self, index: str, body: dict, **kwargs) -> dict | None:
        try:
            doc = await self.elastic.search(index=index, body=body, **kwargs)
        except NotFoundError:
            return None
        return doc
