import uuid
from datetime import datetime

from ..settings import test_settings

movies = [
    {
        "id": str(uuid.uuid4()),
        "imdb_rating": 8.5,
        "genres": [
            {"id": "6659b767-b656-49cf-80b2-6a7c012e9d21", "name": "Action"},
            {"id": "fbd77e08-4dd6-4daf-9276-2abaa709fe87", "name": "Sci-Fi"},
        ],
        "title": "The Star",
        "description": "New World",
        "actors_names": ["Ann", "Bob"],
        "writers_names": ["Ben", "Howard"],
        "directors_names": ["Joe", "John"],
        "actors": [
            {"id": "ef86b8ff-3c82-4d31-ad8e-72b69f4e3f95", "name": "Ann"},
            {"id": "fb111f22-121e-44a7-b78f-b19191810fbf", "name": "Bob"},
        ],
        "writers": [
            {"id": "caf76c67-c0fe-477e-8766-3ab3ff2574b5", "name": "Ben"},
            {"id": "b45bd7bc-2e16-46d5-b125-983d356768c6", "name": "Howard"},
        ],
        "directors": [
            {"id": "caf76c67-c0fe-477e-8766-3ab3ff257666", "name": "Joe"},
            {"id": "b45bd7bc-2e16-46d5-b125-983d35676666", "name": "John"},
        ],
    }
    for _ in range(60)
]

persons = [
    {
        "id": str(uuid.uuid4()),
        "full_name": "John Lucas Junior",
        "films": [
            {
                "id": str(uuid.uuid4()),
                "roles": ["actor"],
                "title": "First Film",
                "imdb_rating": 8.5,
            },
            {
                "id": str(uuid.uuid4()),
                "roles": ["actor", "writer"],
                "title": "Last Film",
                "imdb_rating": 1.0,
            },
        ],
    }
    for _ in range(60)
]

genres = [
    {
        "id": "1ff0d3aa-e4a9-4035-8c48-e48c5f7568e4",
        "name": "Action",
        "description": "",
        "created": datetime.now().isoformat(),
        "modified": datetime.now().isoformat(),
    },
    {
        "id": "35b63763-5ee6-4ad3-8165-852231a09f7c",
        "name": "Adventure",
        "description": "",
        "created": datetime.now().isoformat(),
        "modified": datetime.now().isoformat(),
    },
    {
        "id": "40e45abf-82fb-4ba9-9eac-9505fdb0869b",
        "name": "Fantasy",
        "description": "",
        "created": datetime.now().isoformat(),
        "modified": datetime.now().isoformat(),
    },
    {
        "id": "80e45abf-82fb-4ba9-9eac-9505fdb0869b",
        "name": "Thriller",
        "description": "",
        "created": datetime.now().isoformat(),
        "modified": datetime.now().isoformat(),
    },
    {
        "id": "90e45abf-82fb-4ba9-9eac-9505fdb0869b",
        "name": "Fantasy",
        "description": "",
        "created": datetime.now().isoformat(),
        "modified": datetime.now().isoformat(),
    },
]


def generate_es_data(index, data_items):
    elastic_bulk_data: list[dict] = []
    for row in data_items:
        elastic_bulk_data.append(({"_index": index, "_id": row["id"], "_source": row}))
    return elastic_bulk_data


movies_data = generate_es_data(test_settings.es_index_movies, movies)
persons_data = generate_es_data(test_settings.es_index_persons, persons)
genres_data = generate_es_data(test_settings.es_index_genres, genres)
