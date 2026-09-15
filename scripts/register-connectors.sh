#!/usr/bin/env bash
# Register (or update) Debezium outbox connectors against Kafka Connect.
set -euo pipefail
CONNECT="${CONNECT_URL:-http://localhost:8083}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "waiting for kafka connect at $CONNECT"
for i in $(seq 1 60); do
  if curl -fsS "$CONNECT/" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
curl -fsS "$CONNECT/" >/dev/null

for f in "$ROOT"/deployments/debezium/*.json; do
  name=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "$f")
  echo "upsert $name from $f"
  if ! curl -fsS -X POST -H "Content-Type: application/json" --data @"$f" "$CONNECT/connectors" >/dev/null 2>&1; then
    curl -fsS -X PUT -H "Content-Type: application/json" \
      --data "$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["config"]))' "$f")" \
      "$CONNECT/connectors/$name/config" >/dev/null
  fi
  state=$(curl -fsS "$CONNECT/connectors/$name/status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["connector"]["state"])')
  echo "  connector state: $state"
done

echo "connectors:"
curl -sS "$CONNECT/connectors" | python3 -m json.tool
