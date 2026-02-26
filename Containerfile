FROM python:3.13-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Pipfile Pipfile.lock ./
RUN pip install --no-cache-dir pipenv \
    && pipenv install --system --deploy \
    && pip uninstall -y pipenv virtualenv

COPY config.py run_pipeline.py manage_locations.py ./
COPY pipeline/ pipeline/
COPY parsers/ parsers/
COPY formatters/ formatters/
COPY utils/ utils/

ENTRYPOINT ["python", "run_pipeline.py"]
