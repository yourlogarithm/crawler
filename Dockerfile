FROM python:3.10.13-slim-bookworm as base

WORKDIR /app

COPY ./pyproject.toml ./poetry.lock /app/

RUN pip install --upgrade pip poetry
RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction --no-ansi --no-dev

COPY ./src/* /app

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0"]
