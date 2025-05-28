# Packages

from datetime import datetime
from pydantic import BaseModel, Field
from uuid import UUID

# Class models


class BygningQuery(BaseModel):
    model_config = {"extra": "forbid"}

    id_lokal_id: UUID = Field(
        UUID("918d292d-eb04-4e5d-b9d0-d8026e9e0bd6"),
        description="UUID på den bygning, du ønsker at hente.",
    )
    gyldig: bool = Field(
        True,
        description="Sæt gyldig til True, hvis du kun ønsker at se den seneste registrering for hver historisk virkningsperiode.",
    )
    ibrug: bool = Field(
        True,
        description="Hvis gyldig er True, sæt da ibrug til True, for at hente information om bygningen, som den ser ud i dag.",
    )


class BygningResponse(BaseModel):
    id: UUID
    byg007_bygningsnummer: int | None
    byg021_bygningens_anvendelse: int | None
    id_lokal_id: UUID
    grund: UUID
    virkning_fra: datetime
    registrering_fra: datetime


class ColumnSchema(BaseModel):
    db_column_name: str
    db_type: str | None


class DbSchema(BaseModel):
    db_table_name: str
    columns: dict[str, ColumnSchema]
