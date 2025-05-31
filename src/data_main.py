# Packages import
from psycopg import connect

# Modules import
from src.env import POSTGRES_CONNECTION_STRING
from src.database_creation import map_schema, database_setup
from src.data_load import (
    upload_data,
    upsert_data,
    check_upload_and_file_data_match,
    check_upload_and_api_exposed_data_match,
)

if __name__ == "__main__":
    cnx = connect(POSTGRES_CONNECTION_STRING)
    file_path = "data/BBR_Totaludtraek_DeltaDaily_JSON_HF_20250521080209.json"
    print("Mapping schema...")
    saved_schema = map_schema(file_path)
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
    print("Comparing hashsum of staging and file data...")
    check_upload_and_file_data_match(file_path, saved_schema)
    print("Data match between staging and file data checked.")
    print("Comparing hashsum of staging and api_exposed data...")
    check_upload_and_api_exposed_data_match(saved_schema, cnx)
    print("Data match between staging and api_exposed data checked.")
    cnx.close()
