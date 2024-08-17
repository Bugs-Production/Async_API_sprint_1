from elasticsearch import Elasticsearch

from data_sync.config.config import ElasticSettings
from tests.functional.utils.wait_for_db import wait_for_db

elastic_settings = ElasticSettings()

if __name__ == "__main__":
    es_client = Elasticsearch(hosts=elastic_settings.host)
    wait_for_db(es_client, "Elasticsearch launched")
