import time

from elasticsearch import Elasticsearch

from data_sync.config.config import ElasticSettings

elastic_settings = ElasticSettings()

if __name__ == "__main__":
    es_client = Elasticsearch(hosts=elastic_settings.host)
    while True:
        if es_client.ping():
            print("Elasticsearch launched")
            break
        time.sleep(1)
