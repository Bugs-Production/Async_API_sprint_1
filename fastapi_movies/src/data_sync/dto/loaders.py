from datetime import datetime
from typing import Any, Dict, List

import psycopg
from elasticsearch import Elasticsearch
from pydantic import BaseModel

from dto.extractors import PostgresExtractor
from dto.transformers import ElasticTransformer
from state.state import State
from utils.constants import PG_FETCH_SIZE
from utils.decorators import backoff
from utils.logger import logger


class Postgres:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn

    @backoff()
    def execute(self, sql_path, params: Dict[str, Any]) -> List[dict]:
        with self.conn.cursor() as cursor:
            with open(sql_path, "rb") as f:
                query = f.read()
            cursor.execute(query, params)
            rows = cursor.fetchmany(PG_FETCH_SIZE)
            logger.info(rows)
            return rows


class ElasticLoader:
    @staticmethod
    def load(client: Elasticsearch, index, objects: List[BaseModel]) -> Dict:
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
            logger.info(body)
        res = client.bulk(index=index, body=body)
        logger.info(res)
        return res


class Task:
    def __init__(
        self,
        state_key: str,
        elastic_index: str,
        extractor: PostgresExtractor,
        el_transformer: ElasticTransformer,
        sql_path: str,
    ):
        self.state_key = state_key
        self.elastic_index = elastic_index
        self.extractor = extractor
        self.el_transformer = el_transformer
        self.sql_path = sql_path


class LoadManager:
    def __init__(self, pg: Postgres, elastic: Elasticsearch, state: State):
        self.pg = pg
        self.elastic = elastic
        self.state = state

    def load_to_elastic(self, task: Task):
        last_modified_obj = self.state.get_state(task.state_key, datetime.min)
        while pg_data := self.pg.execute(
            sql_path=task.sql_path, params={"dttm": last_modified_obj}
        ):
            elastic_objects = []
            tmp_last_obj_modified = last_modified_obj
            for obj in pg_data:
                pg_obj = task.extractor.extract(obj)
                elastic_obj = task.el_transformer.transform(pg_obj)
                elastic_objects.append(elastic_obj)
                tmp_last_obj_modified = pg_obj.modified
            res = ElasticLoader.load(
                self.elastic, task.elastic_index, elastic_objects
            )
            if res.get("errors", True):
                logger.error("Elastic loader have a error!")
                break
            last_modified_obj = tmp_last_obj_modified
            self.state.save_state(task.state_key, str(last_modified_obj))
