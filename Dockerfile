ARG PYTHON_BASE="python:3.13-slim-bookworm@sha256:9d7f287598e1a5a978c015ee176d8216435aaf335ed69ac3c38dd1bbb10e8d64"

FROM ${PYTHON_BASE} AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /build

COPY pyproject.toml README.md LICENSE ./
COPY container/constraints.txt ./container/constraints.txt
COPY src ./src

RUN python -m pip install \
        --constraint container/constraints.txt \
        setuptools \
        wheel \
    && python -m pip wheel \
        --constraint container/constraints.txt \
        --no-build-isolation \
        --wheel-dir /wheels \
        . \
    && python -m venv /opt/histdatacom \
    && /opt/histdatacom/bin/python -m pip install \
        --no-index \
        --find-links=/wheels \
        histdatacom \
    && rm -rf /wheels

FROM ${PYTHON_BASE} AS runtime

ARG IMAGE_CREATED="1970-01-01T00:00:00Z"
ARG IMAGE_REVISION="unknown"
ARG IMAGE_VERSION="0.0.0+container"

LABEL org.opencontainers.image.created="${IMAGE_CREATED}" \
      org.opencontainers.image.description="HistData.com FX data acquisition, quality, orchestration, and reconstruction CLI" \
      org.opencontainers.image.documentation="https://github.com/dmidlo/histdata.com-tools/blob/main/docs/container.md" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.revision="${IMAGE_REVISION}" \
      org.opencontainers.image.source="https://github.com/dmidlo/histdata.com-tools" \
      org.opencontainers.image.title="histdatacom" \
      org.opencontainers.image.version="${IMAGE_VERSION}"

ENV HISTDATACOM_RUNTIME_HOME=/workspace/runtime \
    HISTDATACOM_RUNTIME_WORKSPACE=/workspace \
    HISTDATACOM_TEMPORAL_CACHE_DIR=/workspace/cache/temporal-cli \
    HOME=/workspace \
    PATH=/opt/histdatacom/bin:${PATH} \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    XDG_CACHE_HOME=/workspace/cache \
    XDG_STATE_HOME=/workspace/runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends tini=0.19.0-1+b3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 histdatacom \
    && useradd \
        --system \
        --uid 10001 \
        --gid 10001 \
        --home-dir /workspace \
        --shell /usr/sbin/nologin \
        histdatacom \
    && install --directory \
        --owner 10001 \
        --group 10001 \
        /workspace \
        /workspace/cache \
        /workspace/cache/temporal-cli \
        /workspace/data \
        /workspace/runtime

COPY --from=builder /opt/histdatacom /opt/histdatacom

WORKDIR /workspace
USER 10001:10001
STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/bin/tini", "--", "histdatacom"]
CMD ["--help"]
