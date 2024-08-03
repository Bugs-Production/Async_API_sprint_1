from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from api.paginator import Paginator
from .api_models import Genre, GenreDetail
from services.genre import GenreService, get_genre_service


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


@router.get("/{genre_id}", response_model=GenreDetail)
async def genre_details(
    genre_id: str, genre_service: GenreService = Depends(get_genre_service)
) -> GenreDetail:
    genre = await genre_service.get_by_id(genre_id)

    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="genre not found")

    return GenreDetail(**genre.dict())
