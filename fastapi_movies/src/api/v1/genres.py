from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from api.paginator import Paginator
from .api_models import Genre
from services.genre import GenreService, get_genre_service
from uuid import UUID

router = APIRouter()

@router.get("/", response_model=list[Genre])
async def genres(
        paginator: Paginator = Depends(Paginator),
        genre_service: GenreService = Depends(get_genre_service)
) -> list[Genre]:
    all_genres = await genre_service.get_all_genres(
        page_num=paginator.page_number,
        page_size=paginator.page_size,
    )
    if not all_genres:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND,
                            detail="genres not found")

    return [Genre(**genre.model_dump()) for genre in all_genres]
