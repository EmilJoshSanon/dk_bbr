#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
until pg_isready -h db -p 5432 -U postgres_user; do
  echo "Waiting for PostgreSQL to start..."
  sleep 1
done

# Run data_main.py for database setup
poetry run python3 -m src.data_main

# Run tests
poetry run pytest -vv
if [ $? -ne 0 ]; then
  echo "Tests failed, exiting..."
  exit 1
fi

# Start FastAPI server if tests pass
exec "$@"