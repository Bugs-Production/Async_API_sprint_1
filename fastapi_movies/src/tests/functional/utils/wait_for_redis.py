import redis

from core.config import settings as config
from tests.functional.utils.wait_for_db import wait_for_db

if __name__ == "__main__":
    redis_client = redis.Redis(host=config.redis_host, port=config.redis_port)
    wait_for_db(redis_client, "Redis launched")
