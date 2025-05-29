# Packages import
import ijson

from psycopg import Connection, connect
from typing import Any


# Modules import
from src.type_models import DbSchema
from src.env import POSTGRES_CONNECTION_STRING


def upload_data(
    saved_schema: dict[str, DbSchema], cnx: Connection[tuple[Any, ...]], file_path: str
) -> None:
    file = open(file_path, "rb")
    with cnx.cursor() as cur:
        cur.execute("SET search_path TO upload, public")
        for t in saved_schema:
            file.seek(0)
            if saved_schema[t].columns != {}:
                columns = saved_schema[t].columns
                cur.execute(f"TRUNCATE TABLE upload.{saved_schema[t].db_table_name}")
                with cur.copy(
                    f"COPY upload.{saved_schema[t].db_table_name} ({', '.join([columns[c].db_column_name for c in columns])}) FROM STDIN"
                ) as copy:
                    for rec in ijson.items(file, f"{t}.item"):
                        copy.write_row([rec[c] for c in columns])
        cnx.commit()


def upsert_data(
    saved_schema: dict[str, DbSchema], cnx: Connection[tuple[Any, ...]]
) -> None:
    id = "MD5(id_lokal_id || virkning_fra || registrering_fra)::UUID"
    for t in saved_schema:
        if saved_schema[t].columns != {}:
            table = saved_schema[t].db_table_name
            columns = [
                saved_schema[t].columns[c].db_column_name
                for c in saved_schema[t].columns
            ]
            column_types = [
                saved_schema[t].columns[c].db_type for c in saved_schema[t].columns
            ]
            mutable = "||".join("COALESCE(" + c + ", '')" for c in columns)
            mutable = f"MD5({mutable})::UUID"
            select_columns = ", ".join(
                "NULLIF(" + c + ", '')::" + tp for c, tp in zip(columns, column_types)
            )
            query = f"""--sql
                INSERT INTO api_exposed.{table} 
                (id, {', '.join(columns)}, mutable) 
                SELECT {id}, {select_columns}, {mutable}
                FROM upload.{table}
                ON CONFLICT (id) 
                DO UPDATE SET 
                    {', '.join([c + ' = EXCLUDED.' + c for c in columns])},
                    mutable = EXCLUDED.mutable,
                    updated_at = NOW();
            """
            with cnx.cursor() as cur:
                cur.execute(query)
    cnx.commit()


def check_upload_and_file_data_match(
    file_path: str, saved_schema: dict[str, DbSchema]
) -> None:
    for t in saved_schema:
        if saved_schema[t].columns != {}:
            columns = saved_schema[t].columns
            file = open(file_path, "rb")
            md5_hash = hashlib.md5()
            for rec in ijson.items(file, f"{t}.item"):
                md5_hash.update(
                    str(
                        tuple(
                            [rec[c] if rec[c] is None else str(rec[c]) for c in columns]
                        )
                    ).encode("utf-8")
                )
            query = f"""
                SELECT {', '.join([columns[c].db_column_name for c in columns])}
                from upload.{saved_schema[t].db_table_name}
                ORDER BY seq_id
            """
            db_md5_hash = hashlib.md5()
            with connect(POSTGRES_CONNECTION_STRING) as stream_cnx:
                with stream_cnx.cursor(name="stream_cursor") as stream_cur:
                    stream_cur.execute(query)

                    for row in stream_cur:
                        db_md5_hash.update(str(row).encode("utf-8"))
            if md5_hash.hexdigest() == db_md5_hash.hexdigest():
                print(f"{t} match between file and upload table OK!")
            else:
                print(f"{t} mismatch between file and upload table ERROR!")
                exit()
