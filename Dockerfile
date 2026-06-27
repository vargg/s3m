FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-install-project

COPY . .

RUN uv sync --frozen

FROM builder AS test

RUN cp config/settings.example.yaml config/settings.yaml
CMD ["uv", "run", "pytest"]

FROM python:3.12-slim-bookworm AS final

WORKDIR /app

COPY --from=builder /app /app

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD ["granian", "--interface", "asgi", "s3mer.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
