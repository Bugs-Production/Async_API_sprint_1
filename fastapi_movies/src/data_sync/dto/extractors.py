from abc import ABC, abstractmethod

from dto.models import PostgresFilmWork, PostgresGenre, PostgresPerson


class DataExtractor(ABC):
    @abstractmethod
    def extract(self, data: dict):
        """Валидирует данные, пришедшие из БД."""
        pass


class FilmsPostgresExtractor(DataExtractor):
    def extract(self, data: dict) -> PostgresFilmWork:
        film_work = PostgresFilmWork(
            id=data["id"],
            title=data["title"],
            description=data["description"],
            rating=data["rating"],
            type=data["type"],
            created=data["created"],
            modified=data["modified"],
            genres=data["genres"],
            actors=data["actors"],
            directors=data["directors"],
            writers=data["writers"],
        )
        return film_work


class GenresPostgresExtractor(DataExtractor):
    def extract(self, data: dict) -> PostgresGenre:
        genre = PostgresGenre(
            id=data["id"],
            name=data["name"],
            description=data["description"],
            created=data["created"],
            modified=data["modified"],
        )
        return genre


class PersonsPostgresExtractor(DataExtractor):
    def extract(self, data: dict) -> PostgresPerson:
        person = PostgresPerson(
            id=data["id"],
            full_name=data["full_name"],
            films=data["films"],
            modified=data["modified"],
        )
        return person
