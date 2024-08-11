from abc import ABC, abstractmethod
from typing import Any

from dto.models import (ElasticFilmWork, ElasticGenre, ElasticPerson,
                        PostgresFilmWork, PostgresGenre, PostgresPerson)
from utils.utils import create_elastic_objects_list


class Transformer(ABC):
    @abstractmethod
    def transform(self, data) -> Any:
        """
        Приводит данные из постгреса к формату
        для загрузки в Elastic
        :param data: данные, которые нужно привести к формату эластика
        :return: валидированные данные в формате эластика
        """
        pass


class FilmsElasticTransformer(Transformer):
    def transform(self, data: PostgresFilmWork) -> ElasticFilmWork:
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


class GenresElasticTransformer(Transformer):
    def transform(self, data: PostgresGenre) -> ElasticGenre:
        genre = ElasticGenre(
            id=str(data.id),
            name=data.name,
            description=data.description,
            created=data.created.isoformat(),
            modified=data.modified.isoformat(),
        )
        return genre


class PersonsElasticTransformer(Transformer):
    def transform(self, data: PostgresPerson) -> ElasticPerson:
        person = ElasticPerson(
            id=str(data.id),
            full_name=data.full_name,
            films=data.films,
        )
        return person
