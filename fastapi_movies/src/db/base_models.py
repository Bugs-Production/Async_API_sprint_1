from abc import ABC, abstractmethod
from typing import Any, Optional


class AbstractCache(ABC):
    """Абстрактный класс для кэширования"""

    def __init__(self, cache_client):
        self.cache_client = cache_client

    @abstractmethod
    async def get_from_cache(self, key: str) -> Optional[Any]:
        pass

    @abstractmethod
    async def put_to_cache(self, key: str, value: Any, ttl: int) -> None:
        pass

    def create_cache_key(self, *args) -> str:
        return "_".join(str(arg) for arg in args if arg)


class AbstractStorage(ABC):
    """Абстрактный класс для хранения данных сервиса"""

    @abstractmethod
    async def get(self, *args, **kwargs):
        pass

    @abstractmethod
    async def get_batch(self, *args, **kwargs):
        pass
