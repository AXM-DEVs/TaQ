#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose --env-file .env.example -f deploy/docker-compose.yml)
healthy_services=(redpanda redis postgres clickhouse)
running_services=(otel-collector prometheus grafana)
completed_services=(topic-init raw-topic-config-init dead-letter-topic-init context-dead-letter-topic-init source-dead-letter-topic-init approved-actions-topic-init)
deadline=$(( $(date +%s) + 180 ))

while (( $(date +%s) < deadline )); do
  ready=true

  for service in "${healthy_services[@]}" "${running_services[@]}"; do
    container_id=$("${compose[@]}" ps --all --quiet "$service")
    if [[ -z "$container_id" ]]; then
      ready=false
      continue
    fi

    state=$(docker inspect --format '{{.State.Status}}' "$container_id")
    health=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container_id")
    if [[ "$state" != running ]]; then
      ready=false
    elif [[ " ${healthy_services[*]} " == *" $service "* && "$health" != healthy ]]; then
      ready=false
    fi
  done

  for service in "${completed_services[@]}"; do
    container_id=$("${compose[@]}" ps --all --quiet "$service")
    if [[ -z "$container_id" ]]; then
      ready=false
      continue
    fi

    result=$(docker inspect --format '{{.State.Status}} {{.State.ExitCode}}' "$container_id")
    if [[ "$result" != "exited 0" ]]; then
      ready=false
    fi
  done

  if [[ "$ready" == true ]]; then
    echo "Compose listo: dependencias saludables, observabilidad ejecutándose e inicializadores completados."
    exit 0
  fi

  sleep 2
done

echo "El stack Compose no alcanzó el estado listo dentro de 180 segundos." >&2
"${compose[@]}" ps --all >&2 || true
"${compose[@]}" logs --tail=80 >&2 || true
exit 1
