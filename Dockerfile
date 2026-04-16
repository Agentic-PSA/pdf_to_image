FROM python:3.13-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# Install poppler for PDF-to-image conversion (pdf2image)
RUN apt-get update && \
    apt-get install -y --no-install-recommends poppler-utils && \
    rm -rf /var/lib/apt/lists/*

# Install poetry and dependencies
RUN pip install --no-cache-dir poetry
COPY pyproject.toml poetry.lock* ./
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi --no-root

COPY main.py .

# Output directory for chunked jobs
RUN mkdir -p /data/output

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]