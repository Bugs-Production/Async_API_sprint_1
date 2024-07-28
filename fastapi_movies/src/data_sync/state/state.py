from typing import Any


class State:
    def __init__(self, storage):
        self.storage = storage

    def get_state(self, key: str, fallback: Any = None) -> str:
        """Достает данные по ключу из хранилища."""
        value = self.storage.retrieve_state().get(key)
        return value if value else fallback

    def save_state(self, key: str, value: Any) -> None:
        """Сохраняем данные в хранилище."""
        state = self.storage.retrieve_state()
        state[key] = value
        self.storage.save_state(state)
