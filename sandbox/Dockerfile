# Offline test image for FR-8: pytest baked in, so --network none works.
ARG PYTEST_VERSION=9.2.0
FROM python:3.12-slim
ARG PYTEST_VERSION
RUN pip install --no-cache-dir "pytest==${PYTEST_VERSION}"
USER 65534:65534
WORKDIR /tmp
