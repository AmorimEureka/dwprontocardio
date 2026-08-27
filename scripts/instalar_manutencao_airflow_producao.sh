#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
readonly MAINTENANCE_SCRIPT="${SCRIPT_DIR}/manutencao_airflow_producao.sh"
readonly LOG_FILE="${PROJECT_DIR}/logs/manutencao_airflow_producao.log"
readonly CRON_MARKER="# manutencao-airflow-producao"
readonly CRON_SCHEDULE="${AIRFLOW_MAINTENANCE_CRON:-17 3 * * *}"
readonly CACHE_MAX_AGE="${DOCKER_CACHE_MAX_AGE:-168h}"
readonly MIN_FREE_PERCENT="${DOCKER_MIN_FREE_PERCENT:-20}"

mkdir -p "$(dirname "$LOG_FILE")"

current_crontab="$(crontab -l 2>/dev/null || true)"
current_crontab="$(printf '%s\n' "$current_crontab" | sed "\|${CRON_MARKER}|d")"

{
    printf '%s\n' "$current_crontab"
    printf '%s AIRFLOW_PROJECT_DIR=%q DOCKER_CACHE_MAX_AGE=%q DOCKER_MIN_FREE_PERCENT=%q %q >> %q 2>&1 %s\n' \
        "$CRON_SCHEDULE" "$PROJECT_DIR" "$CACHE_MAX_AGE" "$MIN_FREE_PERCENT" \
        "$MAINTENANCE_SCRIPT" "$LOG_FILE" "$CRON_MARKER"
} | sed '/^[[:space:]]*$/d' | crontab -

printf 'Manutenção diária instalada: %s\n' "$CRON_SCHEDULE"
