# Base image
FROM debian:12

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.10 \
    python3.10-dev \
    python3.10-venv \
    python3-pip \
    curl \
    build-essential \
    postgresql \
    postgresql-contrib \
    postgresql-server-dev-all \
    postgresql-client \
    postgis \
    postgresql-14-postgis-3 \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Setup Python and Poetry
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1 \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1 \
    && curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

# Configure PostgreSQL
USER postgres
RUN /etc/init.d/postgresql start && \
    psql --command "CREATE USER docker WITH SUPERUSER PASSWORD 'docker';" && \
    createdb -O docker bbr_db && \
    psql -d bbr_db -c "CREATE EXTENSION postgis;" && \
    psql -d bbr_db -c "CREATE EXTENSION postgis_topology;" && \
    echo "host all  all    0.0.0.0/0  md5" >> /etc/postgresql/14/main/pg_hba.conf && \
    echo "listen_addresses='*'" >> /etc/postgresql/14/main/postgresql.conf

# Switch back to root user
USER root

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Set up environment file
RUN cp example.env .env && \
    sed -i 's/<connection string here>/postgresql:\/\/docker:docker@localhost:5432\/bbr_db/g' .env && \
    sed -i 's/<secret api key here>/test_api_key/g' .env

# Install project dependencies excluding test and dev groups
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --without test,dev

# Expose FastAPI port
EXPOSE 8000

# Start PostgreSQL service and run the application
CMD service postgresql start && \
    python -m src.main && \
    uvicorn src.api_main:app --host 0.0.0.0 --port 8000
