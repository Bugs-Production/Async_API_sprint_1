from abc import ABC, abstractmethod
from datetime import datetime as dt
from typing import Any

import psycopg
from dto.extractors import DataExtractor
from dto.transformers import Transformer
from elasticsearch import Elasticsearch
from pydantic import BaseModel
from state.state import State
from utils.constants import PG_FETCH_SIZE
from utils.decorators import backoff
from utils.logger import logger


class Postgres:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn

    @backoff()
    def execute(self, sql_path, params: dict[str, Any]) -> list[dict]:
        with self.conn.cursor() as cursor:
            with open(sql_path, "rb") as f:
                query = f.read()
            cursor.execute(query, params)
            rows = cursor.fetchmany(PG_FETCH_SIZE)
            return rows


class ElasticLoader:
    @staticmethod
    def load(client: Elasticsearch, index, objects: list[BaseModel]) -> dict:
        """Осуществляет балковую загрузку данных в Эластик."""
        body = []
        for _object in objects:
            body.append(
                {
                    "index": {
                        "_index": index,
                        "_id": _object.id,
                    }
                }
            )
            body.append({**_object.model_dump()})
        res = client.bulk(index=index, body=body)
        logger.info(res)
        return res


class ElasticTask:
    def __init__(
        self,
        state_key: str,
        elastic_index: str,
        extractor: DataExtractor,
        el_transformer: Transformer,
        sql_path: str,
    ):
        """
        Представляет собой задачу на загрузку данных из постгреса в эластик
        :param state_key: ключ для хранилища, по которому ищем дату
        последнего изменения
        :param elastic_index: ключ индекса в эластике
        :param extractor: объект, предназначенный для получения данных из
        постгреса и их валидации
        :param el_transformer: объект, преобразующие данные из extractor к
        формату данных для эластика
        :param sql_path: путь к sql файлу, по которому получаем данные из
        постгреса
        """
        self.state_key = state_key
        self.elastic_index = elastic_index
        self.extractor = extractor
        self.el_transformer = el_transformer
        self.sql_path = sql_path


class LoadManager(ABC):
    """
    Класс, отвечающий за загрузку данных в какую-либо систему
    """

    @abstractmethod
    def load(self):
        pass


class ElasticLoadManager(LoadManager):
    def __init__(self, pg: Postgres, elastic: Elasticsearch, state: State):
        """
        Класс менеджер, отвечающий за непосредственную загрузку данных
        в эластик
        :param pg: объект постгреса, которые отвечает за выполнение sql
        :param elastic: клиент эластика
        :param state: объект хранилища, которые отвечает за получение последней
        сохраненной в эластик записи и записи свежих значений
        """
        self.pg = pg
        self.elastic = elastic
        self.state = state
        self.last_modified_obj = None
        self.tasks = []

    def _create_el_objects(self, task: ElasticTask, pg_data: list[dict]):
        elastic_objects = []
        tmp_last_obj_modified = self.last_modified_obj
        for obj in pg_data:
            pg_obj = task.extractor.extract(obj)
            elastic_obj = task.el_transformer.transform(pg_obj)
            elastic_objects.append(elastic_obj)
            tmp_last_obj_modified = pg_obj.modified
        return elastic_objects, tmp_last_obj_modified

    def add_task(self, task: ElasticTask):
        self.tasks.append(task)

    def load(self):
        for task in self.tasks:
            self.last_modified_obj = self.state.get_state(task.state_key, dt.min)
            while pg_data := self.pg.execute(
                sql_path=task.sql_path, params={"dttm": self.last_modified_obj}
            ):
                el_objects, tmp_last_obj_modified = self._create_el_objects(
                    task, pg_data
                )
                res = ElasticLoader.load(self.elastic, task.elastic_index, el_objects)
                if res.get("errors", True):
                    logger.error("Elastic loader have a error!")
                    break
                self.last_modified_obj = tmp_last_obj_modified
                self.state.save_state(task.state_key, str(self.last_modified_obj))
