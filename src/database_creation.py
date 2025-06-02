# Packages import
import ijson
from psycopg import Connection
from typing import Any
from src.type_models import DbSchema

# Modules import
from src.ressources import sqlify_names, get_type, set_type
from src.type_models import DbSchema, ColumnSchema


# Based on the schema mapped from the json file
# this function creates all the tables in the database.
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
                    # Upload schema columns are all formatted as TEXT simply to make it
                    # easier to check data match between file and upload schema.
                    upload_schema += (
                        f"{saved_schema[t].columns[c].db_column_name} TEXT, "
                    )
                upload_schema += "seq_id SERIAL);"
                cur.execute(upload_schema)
        cur.execute("SET search_path TO api_exposed, public")

        # Aside from different data types the difference between upload and
        # api_exposed are the columns updated_at, created_at, id, and mutable.
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

        cnx.commit()


# This function loops through the entire json file to identify table names,
# column names, and data types.

# We know that the file structure is:
# {
#   "table_name": [
#       {
#           "column_name": value,
#           ...
#       },
#       ...
#   ],
#   ...
# }


# We can then make use of ijson's parser to identify:
# 1. When a new data object (table_name) is started:
#    event == "map_key" and prefix == "".
# 2. When a new row is started ({"column_name": value, ...}):
#    event == "start_map" and prefix.endswith(".item").
# 3. When a row ends: event == "end_map" and
#    prefix == object_prefix.
# 4. What is the key ("column_name") in a key/value pair:
#    event == "map_key", current_object_mapped == False, and
#    current_columns_mapped == False.
# 5. What is the value in a key/value pair: event in
#    ("string", "number", "boolean") and
#    current_object_mapped == False.
def map_schema(file_path: str) -> dict[str, DbSchema]:
    saved_schema: dict[str, DbSchema] = {}
    with open(file_path, "rb") as f:
        parser = ijson.parse(f)
        current_object = None
        object_prefix = None
        current_object_mapped = False
        current_columns_mapped = False
        for prefix, event, value in parser:
            # Start of object (db table)
            if event == "map_key" and prefix == "":
                if current_object is not None:
                    for dt in data_types:
                        saved_schema[current_object].columns[dt].db_type = set_type(
                            data_types[dt]
                        )

                if value not in saved_schema:
                    current_object = value
                    saved_schema[value] = DbSchema(
                        db_table_name=sqlify_names(str(value).replace("List", "")),
                        columns={},
                    )
                    data_types: dict[str, list[str]] = {}
                    current_object_mapped = False
                    current_columns_mapped = False
                    searched_rows = 0
                    last_key = None

            elif not current_object_mapped:
                # Start of row (db table row)
                if event == "start_map" and prefix.endswith(".item"):
                    object_prefix = prefix
                # End of row (db table row)
                elif event == "end_map" and prefix == object_prefix:
                    searched_rows += 1
                    current_columns_mapped = True
                # Key (db table column)
                elif event == "map_key":
                    if not current_columns_mapped:
                        if value not in saved_schema[current_object].columns:
                            saved_schema[current_object].columns[value] = ColumnSchema(
                                db_column_name=sqlify_names(value),
                                db_type=None,
                            )
                            data_types[value] = []
                    last_key = value
                # Value of key (db table column)
                elif event in ("string", "number", "boolean"):
                    if (
                        searched_rows < 1000
                    ):  # To prevent falsey identified data types, each
                        # unique data type found in the first 1000 row is
                        # stored in a list. Finally, the is fed to set_type()
                        # which returns the most "agnostic" data type, e.g.
                        # if both TEXT and INTEGER are found, TEXT is returned.
                        data_types[last_key].append(get_type(str(value)))
                    else:
                        current_object_mapped = True
        for dt in data_types:
            saved_schema[current_object].columns[dt].db_type = set_type(data_types[dt])
    return saved_schema
