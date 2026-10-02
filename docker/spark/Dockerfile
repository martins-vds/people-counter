FROM ghcr.io/astral-sh/uv:0.9.7@sha256:ba4857bf2a068e9bc0e64eed8563b065908a4cd6bfb66b531a9c424c8e25e142 AS uv

FROM apache/spark:4.1.1-scala2.13-java21-python3-ubuntu@sha256:0d4eebff1893f01a24ba1a45c3ee12705264ea5030aef9155547f7062ede0387

ARG PEOPLE_COUNTER_RELEASE_DIGEST=development
ARG DELTA_VERSION=4.2.0

USER root

COPY --from=uv /uv /uvx /usr/local/bin/

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        ffmpeg \
        git \
        libgl1 \
        libglib2.0-0 \
        libsm6 \
        libxext6 \
    && rm -rf /var/lib/apt/lists/*

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON=3.13.9 \
    UV_PYTHON_INSTALL_DIR=/opt/python \
    VIRTUAL_ENV=/opt/people-counter/.venv \
    PATH=/opt/people-counter/.venv/bin:${PATH} \
    PYSPARK_PYTHON=/opt/people-counter/.venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/people-counter/.venv/bin/python \
    SPARK_CONF_DIR=/opt/spark/conf \
    PEOPLE_COUNTER_RELEASE_DIGEST=${PEOPLE_COUNTER_RELEASE_DIGEST} \
    PEOPLE_COUNTER_IMAGE_DIGEST=apache-spark-4.1.1@sha256:0d4eebff1893f01a24ba1a45c3ee12705264ea5030aef9155547f7062ede0387

WORKDIR /opt/people-counter

COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv python install 3.13.9 \
    && uv sync --frozen --no-dev --extra local-spark --extra cpu \
    && uv pip freeze > /opt/people-counter/python-environment.lock

RUN mkdir -p \
        /tmp/ivy \
        /data/output/spark-events \
        /opt/spark/work \
        /opt/spark/scratch \
        /var/lib/people-counter/control \
    && printf 'print("delta dependency resolution complete")\n' > /tmp/resolve_delta.py \
    && spark-submit \
        --master 'local[1]' \
        --packages "io.delta:delta-spark_2.13:${DELTA_VERSION}" \
        --conf spark.jars.ivy=/tmp/ivy \
        /tmp/resolve_delta.py \
    && cp /tmp/ivy/jars/*.jar /opt/spark/jars/ \
    && find /opt/spark/jars -maxdepth 1 -name 'io.delta_*4.2.0.jar' -print \
        | sort > /opt/people-counter/delta-artifacts.lock \
    && test "$(wc -l < /opt/people-counter/delta-artifacts.lock)" -ge 4 \
    && rm -rf /tmp/ivy /tmp/resolve_delta.py /root/.cache/uv

COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY docker/spark/log4j2.properties /opt/spark/conf/log4j2.properties
COPY docker/spark/entrypoint.sh /opt/people-counter/entrypoint.sh

RUN chmod 0555 /opt/people-counter/entrypoint.sh \
    && chown -R 185:185 \
        /opt/people-counter \
        /opt/python \
        /opt/spark/work \
        /opt/spark/scratch \
        /data/output \
        /var/lib/people-counter

LABEL org.opencontainers.image.title="people-counter local Spark" \
      org.opencontainers.image.version="spark-4.1.1-delta-4.2.0" \
      org.opencontainers.image.revision="${PEOPLE_COUNTER_RELEASE_DIGEST}" \
      org.apache.spark.version="4.1.1" \
      io.delta.version="4.2.0"

USER 185:185

ENTRYPOINT ["/opt/people-counter/entrypoint.sh"]
CMD ["client"]
