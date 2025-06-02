from psycopg import connect

from src.env import POSTGRES_CONNECTION_STRING
from src.type_models import BygningQuery, BygningResponse


def get_bygning(query_params: BygningQuery) -> list[BygningResponse]:
    cnx = connect(POSTGRES_CONNECTION_STRING)
    cnx.autocommit = True
    query = """
        SELECT id, byg007_bygningsnummer, byg021_bygningens_anvendelse, id_lokal_id, grund, virkning_fra, registrering_fra
        FROM api_exposed.bygning
        WHERE id_lokal_id = %s
    """
    if query_params.ibrug:
        query += " AND virkning_til IS NULL"
    if query_params.gyldig:
        query += " AND registrering_til IS NULL"
    query += " ORDER BY id_lokal_id, virkning_fra DESC, registrering_fra DESC"
    with cnx.cursor() as cur:
        cur.execute("SET search_path TO api_exposed, public")
        cur.execute(query, (query_params.id_lokal_id,))
        rows = cur.fetchall()
    return [
        BygningResponse(
            id=row[0],
            byg007_bygningsnummer=row[1],
            byg021_bygningens_anvendelse=row[2],
            id_lokal_id=row[3],
            grund=row[4],
            virkning_fra=row[5],
            registrering_fra=row[6],
        )
        for row in rows
    ]
