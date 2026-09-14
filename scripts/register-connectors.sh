#!/usr/bin/env bash
set -euo pipefail
CONNECT="${CONNECT_URL:-http://localhost:8083}"
echo "waiting for kafka connect at $CONNECT"
for i in $(seq 1 60); do
  if curl -fsS "$CONNECT/" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

for f in deployments/debezium/*.json; do
  name=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "$f")
  echo "registering $name from $f"
  curl -sS -X POST -H "Content-Type: application/json" --data @"$f" "$CONNECT/connectors" || true
done

curl -sS "$CONNECT/connectors" | python3 -m json.tool
