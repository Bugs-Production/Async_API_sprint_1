from abc import ABC, abstractmethod

from dto.models import (
    ElasticFilmWork,
    ElasticGenre,
    ElasticPerson,
    PostgresFilmWork,
    PostgresGenre,
    PostgresPerson,
)
from utils.utils import create_elastic_objects_list


class ElasticTransformer(ABC):
    @staticmethod
    @abstractmethod
    def transform(data):
        pass


class FilmsElasticTransformer(ElasticTransformer):
    @staticmethod
    def transform(data: PostgresFilmWork) -> ElasticFilmWork:
        """Приводит данные из объекта PostgresFilmWork в формат
        для загрузки в Elastic.
        """
        el_actors = create_elastic_objects_list(data.actors)

        el_directors = create_elastic_objects_list(data.directors)

        el_writers = create_elastic_objects_list(data.writers)

        genres = create_elastic_objects_list(data.genres)

        film_work = ElasticFilmWork(
            id=str(data.id),
            imdb_rating=data.rating,
            genres=[genre.model_dump() for genre in genres],
            title=data.title,
            description=data.description,
            actors_names=[person.name for person in el_actors],
            directors_names=[person.name for person in el_directors],
            writers_names=[person.name for person in el_writers],
            actors=[person.model_dump() for person in el_actors],
            directors=[person.model_dump() for person in el_directors],
            writers=[person.model_dump() for person in el_writers],
        )

        return film_work


class GenresElasticTransformer(ElasticTransformer):
    @staticmethod
    def transform(data: PostgresGenre) -> ElasticGenre:
        genre = ElasticGenre(
            id=str(data.id),
            name=data.name,
            description=data.description,
            created=data.created.isoformat(),
            modified=data.modified.isoformat(),
        )
        return genre


class PersonsElasticTransformer(ElasticTransformer):
    @staticmethod
    def transform(data: PostgresPerson) -> ElasticPerson:
        person = ElasticPerson(
            id=str(data.uuid),
            full_name=data.full_name,
            films=data.films,
        )
        return person
