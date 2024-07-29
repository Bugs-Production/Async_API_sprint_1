from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException

from .api_models import FilmDetails
from services.film import FilmService, get_film_service

router = APIRouter()


# Внедряем FilmService с помощью Depends(get_film_service)
@router.get("/{film_id}", response_model=FilmDetails)
async def film_details(
    film_id: str, film_service: FilmService = Depends(get_film_service)
) -> FilmDetails:
    film = await film_service.get_by_id(film_id)

    if not film:
        # Если фильм не найден, отдаём 404 статус
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="film not found")

    return FilmDetails(**film.dict())
