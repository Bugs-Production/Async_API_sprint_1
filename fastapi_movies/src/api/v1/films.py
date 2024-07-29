from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi_pagination import Page, add_pagination, paginate

from .api_models import FilmDetails, FilmShort
from services.film import FilmService, get_film_service

router = APIRouter()


@router.get("/", response_model=Page[FilmShort])
async def films(
    sort: str = Query(default="-imdb_rating"),
    film_service: FilmService = Depends(get_film_service)
) -> list[FilmShort]:
    """
    Для сортировки используется default="-imdb_rating" по бизнес логике,
    чтобы всегда выводились только популярные фильмы
    """
    sorted_films = await film_service.get_all_films(sorting=sort)

    return paginate([FilmShort(**film.dict()) for film in sorted_films])


@router.get("/{film_id}", response_model=FilmDetails)
async def film_details(
    film_id: str, film_service: FilmService = Depends(get_film_service)
) -> FilmDetails:
    film = await film_service.get_by_id(film_id)

    if not film:
        # Если фильм не найден, отдаём 404 статус
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="film not found")

    return FilmDetails(**film.dict())

add_pagination(router)
