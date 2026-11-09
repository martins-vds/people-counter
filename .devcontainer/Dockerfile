FROM mcr.microsoft.com/devcontainers/base:bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        git-lfs \
        jq \
        libgl1 \
        libglib2.0-0 \
        libsm6 \
        libxext6 \
        shellcheck \
    && rm -rf /var/lib/apt/lists/*

COPY --from=hashicorp/terraform:1.9.8 /bin/terraform /usr/local/bin/terraform
