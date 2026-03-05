FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip install --upgrade pip \
    && python -m pip install -e . \
    && mkdir -p /app/runtime/logs

VOLUME ["/app/runtime"]

ENTRYPOINT ["qtbot"]
CMD ["status"]
