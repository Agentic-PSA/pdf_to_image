FROM python:3.11-slim

WORKDIR /app

RUN pip install poetry && \
    poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-dev --no-interaction

COPY pdf_to_images.py .

EXPOSE 8002

CMD ["uvicorn", "pdf_to_images:app", "--host", "0.0.0.0", "--port", "8001"]