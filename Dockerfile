FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y gcc g++ libpq-dev python3-dev build-essential && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip setuptools wheel

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/

COPY .env .

ENV PYTHONPATH=/app