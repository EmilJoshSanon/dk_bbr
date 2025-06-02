# Use Python 3.11 slim image (Debian-based)
FROM python:3.11-slim

# Optimize built time and debugging, and reduce image size
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies
# In addition to python, we need psql client to run pg_isready
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

# Set working directory
WORKDIR /app

# Copy Poetry configuration files
COPY pyproject.toml poetry.lock* ./

# Install project dependencies
RUN poetry config virtualenvs.create false && \
    poetry install --no-root --with main,test

# Copy application code
COPY . .

# Configure .env file
RUN cp example.env .env && \
    sed -i 's/<connection string here>/postgresql:\/\/postgres_user:postgres_pass@db:5432\/bbr_db/g' .env && \
    sed -i 's/<secret api key here>/test_api_key/g' .env

# Copy and set up entrypoint
# Since it's only possible to run data_main, when the postgres server is running,
# we entrypoint to check for postgres to be ready and then run data_main.
# The FastAPI server is then started from the docker-compose.yml file.
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Expose FastAPI port
EXPOSE 8000