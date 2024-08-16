from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException

from api.paginator import Paginator
from services.person import PersonService, get_person_service

from .api_models import Film, PersonDetail

router = APIRouter()


@router.get(
    "/search",
    response_model=list[PersonDetail],
    summary="Поиск персоны по имени",
    description="Поиск персон с пагинацией. Если персоны не найдены, возвращается ошибка 404.",
    response_description="Полное имя и список фильмов с ролями персоны.",
)
async def person_search(
    query: str,
    paginator: Paginator = Depends(Paginator),
    person_service: PersonService = Depends(get_person_service),
) -> list[PersonDetail]:

    searched_persons = await person_service.search(
        query=query,
        page_num=paginator.page_number,
        page_size=paginator.page_size,
    )

    if not searched_persons:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="persons not found"
        )

    return [PersonDetail(**person.dict()) for person in searched_persons]


@router.get(
    "/{person_id}",
    response_model=PersonDetail,
    summary="Детали персоны",
    description=(
        "Возвращает подробную информацию о персоне по её UUID. "
        "Если персона не найдена, возвращается ошибка 404."
    ),
    response_description="Полное имя и список фильмов с ролями персоны.",
)
async def person_details(
    person_id: str, person_service: PersonService = Depends(get_person_service)
) -> PersonDetail:
    person = await person_service.get_by_id(person_id)

    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="person not found")

    return PersonDetail(**person.dict())


@router.get(
    "/{person_id}/film",
    response_model=list[Film],
    summary="Фильмы персоны",
    description=(
        "Список фильмов с участием персоны по её UUID. "
        "Если персона или фильмы не найдены, возвращается ошибка 404."
    ),
    response_description="Название фильма и рейтинг",
)
async def person_films(
    person_id: str, person_service: PersonService = Depends(get_person_service)
) -> list[Film]:
    person = await person_service.get_by_id(person_id)

    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="person not found")
    if not person.films:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="films for person not found"
        )

    return [Film(**film.dict()) for film in person.films]
