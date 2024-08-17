import asyncio
from typing import Any

import aiohttp
import pytest_asyncio

from tests.functional.settings import test_settings


@pytest_asyncio.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def aiohttp_session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest_asyncio.fixture(scope="session")
def aiohttp_request(aiohttp_session: aiohttp.ClientSession):
    async def inner(
        method: str, endpoint: str, **kwargs: dict[str, Any]
    ) -> tuple[dict, int]:
        url = test_settings.service_url + endpoint
        async with aiohttp_session.request(
            method=method, url=url, **kwargs
        ) as response:
            body = await response.json()
            status = response.status
            return body, status

    return inner
