# Use Python 3.12.12 slim image
FROM python:3.12.12-slim

# Set working directory
WORKDIR /spotify-forecast-pipeline

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install poetry and add it to path
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    ln -s /root/.local/bin/poetry /usr/local/bin/poetry

# Copy poetry files
COPY pyproject.toml poetry.lock ./

# Configure poetry to not create virtual environment (as we're in container)
RUN poetry config virtualenvs.create false

# Install dependencies
RUN poetry install --no-interaction --no-root

# Copy application code
COPY . .

# Run the application
CMD ["poetry", "run", "python", "-m", "main"]