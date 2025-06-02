# A small prototype of a data pipeline for the Danish Building Registry (BBR)

### Background
This project is made for the sole purpose of providing proof
of skills for the application for the position of Backend Lead Engineer @ Resights.

### Project overview
The project consist of two main modules:

#### 1. data_main.py
This module is the main module of the data pipeline. It is responsible for unzipping the data file, mapping the schema, setting up the database, uploading the data, upserting the data, and finally cleaning up the data.

#### 2. api_main.py
This module is the main module of the API. It is responsible for setting up the FastAPI.

In addition a test module, test_api.py, is provided to test the API. Which is run during the docker build process.

### Setup

To run the project, you need to have docker installed.

Build and run the project with the following command:

```bash
docker-compose up --build
```

When docker is done building, Check out the api endpoint at http://localhost:8000/docs. Token is 'test_api_key'.

To view the data in the database, use your favorite database IDE and connect with the connection string:

DATABASE_URL=postgresql://postgres_user:postgres_pass@<ip of machine where container is running>:5432/postgres

To stop the project, execute the following command:

```bash
docker-compose down
```
