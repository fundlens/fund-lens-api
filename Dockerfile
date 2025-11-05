# Use Python 3.12 slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies (git is needed for git-based Python packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy Poetry files
COPY pyproject.toml poetry.lock ./

# Install Poetry, the export plugin, export dependencies, then clean up
RUN pip install --no-cache-dir poetry poetry-plugin-export && \
    poetry export -f requirements.txt --output requirements.txt --without-hashes && \
    pip install --no-cache-dir -r requirements.txt && \
    pip uninstall -y poetry poetry-plugin-export

# Copy application code
COPY fund_lens_api ./fund_lens_api

# Expose port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "fund_lens_api.main:app", "--host", "0.0.0.0", "--port", "8000"]