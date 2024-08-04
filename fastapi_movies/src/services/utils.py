from typing import Any

CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


def get_offset_params(page_num: int, page_size: int) -> dict[str, int]:
    """Параметры для запроса в Elastic с offset параметрами"""
    offset = (page_num - 1) * page_size
    return {
        "from": offset,
        "size": page_size,
    }


def get_sort_params(sorting: str) -> dict[str, list[dict[str, str]]]:
    """Параметры для запроса в Elastic с сортировкой по рейтингу"""

    return {
        "sort": [{"imdb_rating": "desc" if sorting.startswith("-") else "asc"}],
    }


def get_genre_filter_params(genre_filter: str | None) -> dict[str, Any]:
    """Параметры для запроса в Elastic с фильтрацией по жанру"""

    genre_params = {"query": {}}
    if genre_filter:
        genre_params["query"] = {
            "nested": {
                "path": "genres",
                "query": {"bool": {"should": [{"match": {"genres.id": genre_filter}}]}},
            }
        }
    else:
        genre_params["query"] = {"match_all": {}}

    return genre_params


def get_search_params(field: str, query: str) -> dict[str, Any]:
    """Параметры для запроса в Elastic с простым поисковым запросом по определенному полю"""

    return {
        "query": {
            "match": {
                field: {
                    "query": query,
                }
            }
        }
    }


def create_cache_key_for_films(
    page_num: int, page_size: int, sort: str, genre: str, search: str
) -> str:
    """Создание ключа для кэша фильмов, на основе параметров запроса"""

    cache_key = "films_{page_num}_{page_size}_{sort}_{genre}_{search}".format(
        page_num=page_num if page_num is not None else "",
        page_size=page_size if page_size is not None else "",
        sort=sort if sort is not None else "",
        genre=genre if genre is not None else "",
        search=search if search is not None else "",
    ).strip("_")

    return cache_key
