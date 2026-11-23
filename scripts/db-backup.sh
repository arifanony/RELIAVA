#!/bin/sh
# Single-database DR backup (NFR-5: RPO 15 min, RTO 2 h from this dump).
# Writes OUTSIDE the repo so gate runs never mutate the source tree.
set -eu
CONTAINER="${PG_CONTAINER:-ai-sre-pg16}"
DEST="${BACKUP_DIR:-/tmp/ai-sre-backups}"
mkdir -p "$DEST"
STAMP="$(date +%Y%m%dT%H%M%S)"
OUT="$DEST/aisre-$STAMP.dump"
docker exec "$CONTAINER" pg_dump -Fc -U postgres aisre > "$OUT"
docker cp "$OUT" "$CONTAINER:/tmp/drill.dump"
docker exec "$CONTAINER" pg_restore --list /tmp/drill.dump
