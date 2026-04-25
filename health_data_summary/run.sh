#!/usr/bin/env bash
# health_data_summary — ductile plugin (protocol v2)
#
# Wrapper that pipes the ductile request envelope through `docker run --rm -i`
# to the healthdata image. The actual ETL lives in the image's integrate.py
# (source: github.com/mattjoyce/healthdata).
#
# Volume contract: the host's healthdata tree (garmin.db, withings.db, summary.db
# parent dir) is bind-mounted at /app/data/healthdata in the container. The
# request envelope's config.*_db_path values must resolve inside that mount.
#
# Config keys (consumed from the ductile request):
#   image                    healthdata image tag (default: healthdata:latest)
#   host_healthdata_dir      host-side path of the healthdata tree
#                            (default: /mnt/user/Projects/healthdata)
#   container_healthdata_dir mount target inside the container
#                            (default: /app/data/healthdata)

set -euo pipefail

REQUEST="$(cat)"

IMAGE=$(printf '%s' "$REQUEST" | jq -r '.config.image // "ductile-healthdata:latest"')
HOST_DIR=$(printf '%s' "$REQUEST" | jq -r '.config.host_healthdata_dir // "/mnt/user/Projects/healthdata"')
CONT_DIR=$(printf '%s' "$REQUEST" | jq -r '.config.container_healthdata_dir // "/app/data/healthdata"')

err_response() {
  jq -n --arg msg "$1" \
    '{status:"error", error:$msg, retry:false, logs:[{level:"error", message:$msg}]}'
}

if ! command -v docker >/dev/null 2>&1; then
  err_response "docker CLI not available in plugin runtime"
  exit 0
fi

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  err_response "ductile-healthdata image not found: $IMAGE — build it from github.com/mattjoyce/ductile-healthdata"
  exit 0
fi

exec docker run --rm -i \
  -v "$HOST_DIR:$CONT_DIR" \
  "$IMAGE" <<<"$REQUEST"
