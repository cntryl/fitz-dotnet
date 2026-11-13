#!/usr/bin/env bash
# Blocks until the brokers started by `docker compose up -d` answer, or fails after a timeout.
#
# compose.yml disables container healthchecks, and `docker compose up -d` returns as soon
# as the containers exist, not when the brokers listen. A published port also accepts
# connections (through the Docker proxy) before the broker inside is ready, and the broker
# answers plain HTTP requests while its data plane is still starting: it rejects client
# connections with 503 until then. So each broker's `/readyz` must return success, which it
# does only once the data plane accepts clients, then its TCP port must accept a connection.
#
# Usage: wait-for-broker.sh [timeout-seconds]   (default 90)
# Ports follow compose.yml and honour the same FITZ_*_HOST_*_PORT overrides.
set -euo pipefail

timeout_seconds="${1:-90}"
deadline=$((SECONDS + timeout_seconds))

http_ports=("${FITZ_AUTH_HOST_HTTP_PORT:-4090}" "${FITZ_ANON_HOST_HTTP_PORT:-4190}")
tcp_ports=("${FITZ_AUTH_HOST_TCP_PORT:-4091}" "${FITZ_ANON_HOST_TCP_PORT:-4191}")

wait_until() {
  local description="$1"
  shift
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "Timed out after ${timeout_seconds}s waiting for ${description}" >&2
      docker compose ps >&2 || true
      docker compose logs --tail 50 >&2 || true
      exit 1
    fi
    sleep 1
  done
  echo "ready: ${description}"
}

tcp_open() {
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

for port in "${http_ports[@]}"; do
  wait_until "broker ready on 127.0.0.1:${port}" curl --silent --fail --output /dev/null --max-time 2 "http://127.0.0.1:${port}/readyz"
done
for port in "${tcp_ports[@]}"; do
  wait_until "TCP 127.0.0.1:${port}" tcp_open "${port}"
done
