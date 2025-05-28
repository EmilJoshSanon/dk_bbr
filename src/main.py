# 1. Try to upload data to DB
# 2. If upload fails, check if fail is caused by missing table
# 3. If missing table, dump schema from JSON and create table
# 4. If upload fails, check if fail is caused by schema mismatch
# 5. If schema mismatch, generate schema and change mapping between JSON and DB
# 6. Try to upload data to DB again


# %%
# Packages

from psycopg import Connection, connect
from typing import Any

# Modules

from src.env import POSTGRES_CONNECTION_STRING
from src.ressources import generate_schema, sqlify_names, get_type, set_type
from src.type_models import DbSchema, ColumnSchema

# %%
cnx = connect(POSTGRES_CONNECTION_STRING)
cnx.autocommit = True
file_str = "/home/emil/emil-kwiatek/data/BBR_Totaludtraek_DeltaDaily_JSON_HF_20250521080209.json"

# %%
js_schema = generate_schema(file_str)

# %%
tables: dict[str, list[str]] = {}
for t in js_schema["fields"]:
    tables[t["name"]] = []
    if "fields" in t["type"]["elementType"]:
        for c in t["type"]["elementType"]["fields"]:
            tables[t["name"]].append(c["name"])

# %%
file = open(file_str, "rb")
saved_schema: dict[str, DbSchema] = {}
for t in tables:
    if t not in saved_schema:
        saved_schema[t] = DbSchema(
            db_table_name=sqlify_names(t.replace("List", "")),
            columns={},
        )
        for c in tables[t]:
            saved_schema[t].columns[c] = ColumnSchema(
                db_column_name=sqlify_names(c),
                db_type=None,
            )
            searched_rows = 0
            data_types: list[str] = []
            file.seek(0)
            for record in ijson.items(file, f"{t}.item"):
                if c in record:
                    if record[c] is not None:
                        data_types.append(get_type(str(record[c])))
                    searched_rows += 1
                if searched_rows == 1000:
                    break
            saved_schema[t].columns[c].db_type = set_type(data_types)

# %%
saved_schema


# %%
def database_setup(
    saved_schema: dict[str, DbSchema], cnx: Connection[tuple[Any, ...]]
) -> None:
    with cnx.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        cur.execute("CREATE SCHEMA IF NOT EXISTS upload")
        cur.execute("CREATE SCHEMA IF NOT EXISTS api_exposed")
        cur.execute("SET search_path TO upload, public")
        for t in saved_schema:
            if saved_schema[t].columns != {}:
                upload_schema = f"CREATE TABLE IF NOT EXISTS upload.{saved_schema[t].db_table_name} ("
                for c in saved_schema[t].columns:
                    upload_schema += f"{saved_schema[t].columns[c].db_column_name} {saved_schema[t].columns[c].db_type}, "
                upload_schema = upload_schema[:-2] + ");"
                cur.execute(upload_schema)
        cur.execute("SET search_path TO api_exposed, public")
        for t in saved_schema:
            if saved_schema[t].columns != {}:
                api_exposed_schema = f"CREATE TABLE IF NOT EXISTS api_exposed.{saved_schema[t].db_table_name} (id UUID PRIMARY KEY, "
                for c in saved_schema[t].columns:
                    api_exposed_schema += f"{saved_schema[t].columns[c].db_column_name} {saved_schema[t].columns[c].db_type}, "
                api_exposed_schema += """
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(), 
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(), 
                    mutable UUID NOT NULL);
                """
                cur.execute(api_exposed_schema)


# %%
database_setup(saved_schema, cnx)

# %%
saved_schema


# %%
def upload_data(
    saved_schema: dict[str, DbSchema], cnx: Connection[tuple[Any, ...]]
) -> None:
    file = open(file_str, "rb")
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


# %%
upload_data(saved_schema, cnx)


# %%
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


# %%
upsert_data(saved_schema, cnx)


# %%
saved_schema["BygningList"].columns["byg404Koordinat"]

# %%
