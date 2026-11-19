FROM python:3.13-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt requirements-spark.txt ./
RUN pip install --no-cache-dir -r requirements-spark.txt
COPY clinical_trials ./clinical_trials
COPY sql ./sql

ENTRYPOINT ["python", "-m", "clinical_trials"]
