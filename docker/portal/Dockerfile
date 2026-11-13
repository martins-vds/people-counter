FROM ghcr.io/astral-sh/uv:0.9.7@sha256:ba4857bf2a068e9bc0e64eed8563b065908a4cd6bfb66b531a9c424c8e25e142 AS uv
FROM python:3.13-slim-bookworm@sha256:a5a7017efc5969e00ae2b91c8eb0631049a569652aa4ffc4501769049ae45c72

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl ffmpeg libgl1 libglib2.0-0 \
    && mkdir -p /etc/apt/keyrings \
    && curl --fail --silent --show-error https://packages.microsoft.com/keys/microsoft.asc \
        -o /etc/apt/keyrings/microsoft.asc \
    && printf 'deb [arch=%s signed-by=/etc/apt/keyrings/microsoft.asc] https://packages.microsoft.com/repos/azure-cli/ bookworm main\n' \
        "$(dpkg --print-architecture)" > /etc/apt/sources.list.d/azure-cli.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends azure-cli \
    && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /opt/people-counter
ENV UV_PYTHON=3.13 \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_CACHE_DIR=/root/.cache/uv \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH=/opt/people-counter/.venv/bin:${PATH} \
    HOME=/var/lib/people-counter/portal \
    AZURE_CONFIG_DIR=/var/lib/people-counter/azure \
    PC_PORTAL_SOURCE_ROOT=/data/videos \
    PC_PORTAL_MODELS_ROOT=/data/models \
    PC_PORTAL_STATE_DIR=/var/lib/people-counter/portal
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project --extra portal --extra publisher --extra demo
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --extra portal --extra publisher --extra demo \
    && useradd --uid 10001 --no-create-home portal \
    && mkdir -p /data/videos /data/models /var/lib/people-counter/portal /var/lib/people-counter/azure \
    && chown -R 10001:10001 /var/lib/people-counter
USER 10001:10001
EXPOSE 8088
CMD ["pc-admin", "--container-loopback-publish", "--port", "8088"]
