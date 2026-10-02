#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
exec sudo docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  -f docker-compose.monitoring.yml \
  -f docker-compose.storage.yml \
  -f docker-compose.cutover.yml \
  -f docker-compose.azure.yml \
  -f docker-compose.azure-images.json "$@"
