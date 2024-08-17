import pytest_asyncio
import redis.asyncio as redis

from tests.functional.settings import test_settings


@pytest_asyncio.fixture(scope="function")
async def redis_client():
    pool = redis.ConnectionPool.from_url(
        f"redis://{test_settings.redis_host}:{test_settings.redis_port}"
    )
    client = redis.Redis.from_pool(pool)
    yield client
    await client.aclose()


@pytest_asyncio.fixture(scope="function")
async def redis_flushall(redis_client):
    await redis_client.flushall()
