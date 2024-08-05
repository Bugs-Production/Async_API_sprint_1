from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException

from api.paginator import Paginator
from services.film import FilmService, get_film_service

from .api_models import Film, FilmDetail

router = APIRouter()


@router.get(
    "/search",
    response_model=list[Film],
    summary="Поиск фильмов по названию",
    description="Эндпоинт для поиска фильмов по заданному названию."
    " Пользователь может указать параметры сортировки (по популярности), номер страницы "
    "и количество фильмов на странице для получения результатов."
    " Если фильмы не найдены, возвращается пустой список.",
)
async def film_search(
    query: str,
    sort: str = "-imdb_rating",
    paginator: Paginator = Depends(Paginator),
    film_service: FilmService = Depends(get_film_service),
) -> list[Film]:

    searched_films = await film_service.search_films(
        query=query,
        sorting=sort,
        page_num=paginator.page_number,
        page_size=paginator.page_size,
    )

    if not searched_films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="films not found")

    return [Film(**film.dict()) for film in searched_films]


@router.get(
    "/",
    response_model=list[Film],
    summary="Все фильмы",
    description="Возвращает список всех фильмов с возможностью"
    " сортировки (популярности), номеру страницы, количество фильмов на странице и"
    " фильтрации по жанрам.",
)
async def films(
    sort: str | None = "-imdb_rating",
    genre: str | None = None,
    paginator: Paginator = Depends(Paginator),
    film_service: FilmService = Depends(get_film_service),
) -> list[Film]:
    """
    Для сортировки используется default="-imdb_rating" по бизнес логике,
    чтобы всегда выводились только популярные фильмы
    """
    all_films = await film_service.get_all_films(
        sorting=sort,
        genre_filter=genre,
        page_num=paginator.page_number,
        page_size=paginator.page_size,
    )

    if not all_films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="films not found")

    return [Film(**film.dict()) for film in all_films]


@router.get(
    "/{film_id}",
    response_model=FilmDetail,
    summary="Поиск фильма по UUID",
    description="Возвращает подробную информацию о фильме"
    " по его уникальному идентификатору (UUID)."
    " Если фильм не найден, возвращается ошибка 404.",
)
async def film_details(
    film_id: str, film_service: FilmService = Depends(get_film_service)
) -> FilmDetail:
    film = await film_service.get_by_id(film_id)

    if not film:
        # Если фильм не найден, отдаём 404 статус
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="film not found")

    return FilmDetail(**film.dict())
