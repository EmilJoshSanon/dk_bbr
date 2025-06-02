from fastapi import FastAPI, Query, Header, HTTPException
from typing import Annotated

from src.type_models import BygningQuery, BygningResponse
from src.api_sql_queries import get_bygning
from src.env import API_KEY

app = FastAPI()


@app.post("/bygning/", response_model=list[BygningResponse])
async def read_root(
    query_params: Annotated[BygningQuery, Query()], token: Annotated[str, Header()]
) -> list[BygningResponse]:
    if token != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid token")
    return get_bygning(query_params)
