import json
from json import JSONDecodeError
from typing import Optional

from .base_storage import BaseStorage


class JsonStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = "storage.json"):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        with open(self.file_path, "w") as f:
            json.dump(state, f)

    def retrieve_state(self) -> dict:
        try:
            with open(self.file_path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, JSONDecodeError):
            return dict()
