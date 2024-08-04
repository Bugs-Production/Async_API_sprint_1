from typing import Annotated

from fastapi import Query
from pydantic import BaseModel


class Paginator(BaseModel):
    page_number: Annotated[int, Query(default=1, gt=0)]
    page_size: Annotated[int, Query(default=50, gt=0)]
