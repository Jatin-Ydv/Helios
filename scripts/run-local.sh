#!/usr/bin/env bash
# Run Helios binaries against compose infra (postgres/redis/kafka/jaeger/es).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
mkdir -p "$ROOT/tmp/logs"
export ENVIRONMENT=local
export LOG_LEVEL=info
export JWT_SECRET=helios-dev-only-change-me
export JWT_ISSUER=helios-identity
export INTERNAL_TOKEN=helios-internal-dev
export REDIS_ADDR=localhost:6379
export KAFKA_BROKERS=localhost:9094
export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4318
export ELASTICSEARCH_URL=http://localhost:9200

start() {
  local name="$1" port="$2" dsn="${3:-}"
  extra=()
  if [[ -n "$dsn" ]]; then
    extra+=(env "POSTGRES_DSN=$dsn")
  fi
  echo "starting $name on :$port"
  HTTP_ADDR=":$port" "${extra[@]}" "$ROOT/bin/$name" >"$ROOT/tmp/logs/$name.log" 2>&1 &
  echo $! >"$ROOT/tmp/logs/$name.pid"
}

start identity 8101 "postgres://helios:helios@localhost:5432/identity?sslmode=disable"
start catalog 8102 "postgres://helios:helios@localhost:5432/catalog?sslmode=disable"
start order 8103 "postgres://helios:helios@localhost:5432/orders?sslmode=disable"
start inventory 8104 "postgres://helios:helios@localhost:5432/inventory?sslmode=disable"
start payment 8105 "postgres://helios:helios@localhost:5432/payment?sslmode=disable"
start fulfillment 8106 "postgres://helios:helios@localhost:5432/fulfillment?sslmode=disable"
ORDER_URL=http://localhost:8103 \
INVENTORY_URL=http://localhost:8104 \
PAYMENT_URL=http://localhost:8105 \
FULFILLMENT_URL=http://localhost:8106 \
HTTP_ADDR=:8107 POSTGRES_DSN="postgres://helios:helios@localhost:5432/orchestrator?sslmode=disable" \
  "$ROOT/bin/orchestrator" >"$ROOT/tmp/logs/orchestrator.log" 2>&1 &
echo $! >"$ROOT/tmp/logs/orchestrator.pid"
HTTP_ADDR=:8108 "$ROOT/bin/search" >"$ROOT/tmp/logs/search.log" 2>&1 &
echo $! >"$ROOT/tmp/logs/search.pid"
start notification 8109 "postgres://helios:helios@localhost:5432/notification?sslmode=disable"

IDENTITY_URL=http://localhost:8101 \
CATALOG_URL=http://localhost:8102 \
SEARCH_URL=http://localhost:8108 \
INVENTORY_URL=http://localhost:8104 \
ORDER_URL=http://localhost:8103 \
PAYMENT_URL=http://localhost:8105 \
FULFILLMENT_URL=http://localhost:8106 \
ORCHESTRATOR_URL=http://localhost:8107 \
NOTIFICATION_URL=http://localhost:8109 \
HTTP_ADDR=:8080 "$ROOT/bin/gateway" >"$ROOT/tmp/logs/gateway.log" 2>&1 &
echo $! >"$ROOT/tmp/logs/gateway.pid"

echo "waiting for gateway /healthz"
for i in $(seq 1 40); do
  if curl -fsS http://localhost:8080/healthz >/dev/null 2>&1; then
    echo "gateway ready"
    exit 0
  fi
  sleep 0.5
done
echo "gateway did not become ready; last logs:"
tail -n 40 "$ROOT/tmp/logs"/*.log || true
exit 1
