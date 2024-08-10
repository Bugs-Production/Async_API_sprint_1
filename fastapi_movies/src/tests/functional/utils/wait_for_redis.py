import time

import redis

from core.config import settings as config

if __name__ == "__main__":
    redis_client = redis.Redis(host=config.redis_host, port=config.redis_port)
    while True:
        if redis_client.ping():
            print("Redis launched")
            break
        time.sleep(1)
