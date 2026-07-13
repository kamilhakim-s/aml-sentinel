FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .

# data/ is mounted at runtime (generate it with `make data` first)
ENTRYPOINT ["aml-sentinel"]
