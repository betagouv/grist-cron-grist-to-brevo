FROM python:3.14.0rc3-alpine3.22

WORKDIR /app

# install dependencies
RUN apk add libpq
COPY uv.lock pyproject.toml ./
RUN --mount=from=ghcr.io/astral-sh/uv,source=/uv,target=/bin/uv uv export --no-dev --locked > requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# install source
COPY src src

ENTRYPOINT [ "python", "src/cron/cron.py" ]

