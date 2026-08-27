#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly PROJECT_DIR="${AIRFLOW_PROJECT_DIR:-$(dirname "$SCRIPT_DIR")}"
readonly CACHE_MAX_AGE="${DOCKER_CACHE_MAX_AGE:-168h}"
readonly MIN_FREE_PERCENT="${DOCKER_MIN_FREE_PERCENT:-20}"

log() {
    printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

validate_percent() {
    if [[ ! "$MIN_FREE_PERCENT" =~ ^[0-9]+$ ]] || (( MIN_FREE_PERCENT < 1 || MIN_FREE_PERCENT > 99 )); then
        log "DOCKER_MIN_FREE_PERCENT deve ser um inteiro entre 1 e 99."
        exit 2
    fi
}

free_percent() {
    local used_percent
    used_percent="$(df -P /var/lib/docker | awk 'END {gsub(/%/, "", $5); print $5}')"
    printf '%s\n' "$((100 - used_percent))"
}

prune_build_cache() {
    local available_percent prune_summary
    available_percent="$(free_percent)"

    if (( available_percent < MIN_FREE_PERCENT )); then
        log "Espaço livre em ${available_percent}%; removendo todo o cache de build inativo."
        prune_summary="$(docker builder prune --all --force | tail -n 1)"
    else
        log "Espaço livre em ${available_percent}%; removendo cache de build inativo com mais de ${CACHE_MAX_AGE}."
        prune_summary="$(docker builder prune --all --force --filter "until=${CACHE_MAX_AGE}" | tail -n 1)"
    fi

    log "$prune_summary"
}

scheduler_containers() {
    docker ps --quiet \
        --filter "label=com.docker.compose.project.working_dir=${PROJECT_DIR}" \
        --filter "label=com.docker.compose.service=scheduler"
}

scheduler_is_healthy() {
    local container_id="$1"
    docker exec "$container_id" \
        airflow jobs check --job-type SchedulerJob --local >/dev/null 2>&1
}

wait_for_scheduler() {
    local container_id="$1"
    local attempt

    for attempt in {1..18}; do
        if scheduler_is_healthy "$container_id"; then
            return 0
        fi
        sleep 5
    done

    return 1
}

recover_schedulers() {
    local container_id
    local found=0

    while IFS= read -r container_id; do
        [[ -z "$container_id" ]] && continue
        found=1

        if scheduler_is_healthy "$container_id"; then
            log "Scheduler ${container_id:0:12} saudável."
            continue
        fi

        log "Scheduler ${container_id:0:12} sem heartbeat; reiniciando o container."
        docker restart "$container_id" >/dev/null

        if ! wait_for_scheduler "$container_id"; then
            log "Scheduler ${container_id:0:12} não recuperou o heartbeat no prazo esperado."
            return 1
        fi

        log "Scheduler ${container_id:0:12} recuperado."
    done < <(scheduler_containers)

    if (( found == 0 )); then
        log "Nenhum scheduler ativo foi encontrado para ${PROJECT_DIR}."
        return 1
    fi
}

main() {
    validate_percent
    prune_build_cache
    recover_schedulers
}

main "$@"
