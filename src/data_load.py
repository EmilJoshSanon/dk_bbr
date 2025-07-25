# Datapipline: json_file -> upload schema -> api_exposed schema
# The reason why we don't upload data directly to the api_exposed schema
# is to have the ability to make an upsert with postgres' builtin
# "ON CONFLICT DO UPDATE" and hashsums. This is the fastest way to identify
# new data and update existing rows that have changed.

import hashlib
import ijson
import os

from psycopg import Connection, connect
from typing import Any

from src.type_models import DbSchema
from src.env import POSTGRES_CONNECTION_STRING

# Terminal font colors
RED = "\033[31m"  # Red text
GREEN = "\033[32m"  # Green text
RESET = "\033[0m"  # Reset to default color


# For each data object found in the json file, upload data entries in the
# object to the corresponding table in the postgres database.
# We are using psycopg's copy method for fastest bulk load from file to db.
# Setting search path to the upload schema to ensure data is uploaded to
# the correct schema
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


# Every table in the api_exposed schema is updated by dynamically creating
# an query that selects data from the corresponding table in the upload
# schema and upserting the result se into the api_exposed table.
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
            # mutable is used to identify if a row has changed
            mutable = "||".join("COALESCE(" + c + ", '')" for c in columns)
            mutable = f"MD5({mutable})::UUID"
            select_columns = ", ".join(
                "NULLIF(" + c + ", '')::" + tp for c, tp in zip(columns, column_types)
            )
            query = f"""
                INSERT INTO api_exposed.{table}
                (id, {', '.join(columns)}, mutable)
                SELECT {id}, {select_columns}, {mutable}
                FROM upload.{table}
                ON CONFLICT (id)
                DO
                    UPDATE SET
                        {', '.join([c + ' = EXCLUDED.' + c for c in columns])},
                        mutable = EXCLUDED.mutable,
                        updated_at = NOW()
                    WHERE
                        api_exposed.{table}.mutable <> EXCLUDED.mutable;
            """
            with cnx.cursor() as cur:
                cur.execute(query)
    cnx.commit()


# After upload we check if data matches between the file and the upload schema
# for each table in the upload schema, by calculating the hashsum of the entire
# uploaded data.
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
            query = f"""--sql
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
                print(f"{t} match between file and upload table {GREEN}OK{RESET}!")
            else:
                print(f"{t} mismatch between file and upload table {RED}ERROR{RESET}!")
                exit()


# After upsert we check if data matches between the upload schema and the
# api_exposed schema, by calculating the hashsum of the entire upserted data.
def check_upload_and_api_exposed_data_match(
    saved_schema: dict[str, DbSchema], cnx: Connection[tuple[Any, ...]]
) -> None:
    for t in saved_schema:
        if saved_schema[t].columns != {}:
            columns = saved_schema[t].columns
            with cnx.cursor() as cur:
                hash_columns_str = ", ".join(
                    [
                        "COALESCE(" + columns[c].db_column_name + "::text, '')"
                        for c in columns
                    ]
                )
                cur.execute(
                    f"""
                    SELECT MD5(STRING_AGG(ROW_TO_JSON(t)::TEXT, '')) AS checksum
                    from (
                        select 
                            {hash_columns_str}
                        from api_exposed.{saved_schema[t].db_table_name} b
                        where b.updated_at = (select max(updated_at) from api_exposed.{saved_schema[t].db_table_name})
                        order by 
                            b.id_lokal_id, 
                            b.virkning_fra, 
                            b.registrering_fra
                    ) t;
                """
                )
                api_table_md5_hash = cur.fetchone()[0]
                hash_columns_str = ", ".join(
                    [
                        "COALESCE("
                        + columns[c].db_column_name
                        + "::"
                        + columns[c].db_type
                        + "::TEXT, '')"
                        for c in columns
                    ]
                )
                cur.execute(
                    f"""
                    SELECT MD5(STRING_AGG(ROW_TO_JSON(t)::TEXT, '')) AS checksum
                    from (
                        select 
                            {hash_columns_str}
                        from upload.{saved_schema[t].db_table_name} b 
                        order by 
                            b.id_lokal_id::UUID, 
                            b.virkning_fra::TIMESTAMPTZ, 
                            b.registrering_fra::TIMESTAMPTZ
                    ) t;
                """
                )
                upload_table_md5_hash = cur.fetchone()[0]
            if api_table_md5_hash == upload_table_md5_hash:
                print(
                    f"{saved_schema[t].db_table_name} match between upload and api_exposed table {GREEN}OK{RESET}!"
                )
            else:
                print(
                    f"{saved_schema[t].db_table_name} mismatch between upload and api_exposed table {RED}ERROR{RESET}!"
                )
                exit()


# We do a cleanup, where each table in the upload schema is truncated.
# Since this is only a data staging schema, we erase the data to keep a lean db.
# Also the json file is deleted to save storage.
def cleanup(cnx: Connection[tuple[Any, ...]], file_path: str) -> None:
    with cnx.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'upload'
            AND table_type = 'BASE TABLE';
        """
        )
        tables = cur.fetchall()
        for table in tables:
            cur.execute(f"TRUNCATE TABLE upload.{table[0]} RESTART IDENTITY")
        os.remove(file_path)
    cnx.commit()
