# Packages import
from psycopg import connect
from typing import Any

# Modules import
from src.env import POSTGRES_CONNECTION_STRING
from src.database_creation import map_schema, database_setup
from src.data_load import upload_data, upsert_data, check_upload_and_file_data_match

if __name__ == "__main__":
    cnx = connect(POSTGRES_CONNECTION_STRING)
    file_path = "/home/emil/emil-kwiatek/data/BBR_Totaludtraek_DeltaDaily_JSON_HF_20250521080209.json"
    print("Mapping schema...")
    saved_schema = map_schema(file_path)
    print(saved_schema)
    cnx.close()
    exit()
    print("Schema mapped.")
    print("Setting up database...")
    database_setup(saved_schema, cnx)
    print("Database setup completed.")
    print("Uploading data...")
    upload_data(saved_schema, cnx, file_path)
    print("Data uploaded.")
    print("Upserting data...")
    upsert_data(saved_schema, cnx)
    print("Data upserted.")
    print("Checking data...")
    check_upload_and_file_data_match(file_path, saved_schema)
    print("Data checked.")
    cnx.close()
