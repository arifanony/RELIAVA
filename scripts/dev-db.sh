#!/bin/sh
# Start local Postgres 16 for the M0 gates (needed now to run pytest).
set -eu
if ! docker ps --filter name=ai-sre-pg16 --format '{{.Names}}' 2>/dev/null | grep -q ai-sre-pg16; then
  docker run -d --name ai-sre-pg16 \
    -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=aisre \
    -p 5432:5432 postgres:16-alpine
fi
