#!/usr/bin/env bash
# End-to-end checkout against the local gateway. Demonstrates identity → catalog
# → inventory → orchestrated saga (reserve, authorize, confirm, allocate).
set -euo pipefail
GW="${GATEWAY_URL:-http://localhost:8080}"

echo "== register tenant"
REG=$(curl -sS -X POST "$GW/v1/auth/register" -H 'Content-Type: application/json' -d '{
  "tenant_name": "Helios Labs",
  "email": "founder@helios.dev",
  "password": "correct-horse-battery-staple"
}')
echo "$REG"
TOKEN=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["tokens"]["access_token"])' <<<"$REG")
AUTH="Authorization: Bearer $TOKEN"

echo "== create product"
PROD=$(curl -sS -X POST "$GW/v1/products" -H "$AUTH" -H 'Content-Type: application/json' -d '{
  "sku": "SKU-HELIOS-1",
  "name": "Helios Observability Mug",
  "description": "You cannot improve what you cannot trace.",
  "amount": 2900,
  "currency": "USD",
  "attributes": {"material": "ceramic"}
}')
echo "$PROD"

echo "== stock inventory"
curl -sS -X POST "$GW/v1/inventory/stock" -H "$AUTH" -H 'Content-Type: application/json' -d '{
  "sku": "SKU-HELIOS-1",
  "warehouse_id": "wh-default",
  "on_hand": 50
}'
echo

echo "== checkout"
CHECKOUT=$(curl -sS -X POST "$GW/v1/checkout" -H "$AUTH" -H 'Idempotency-Key: demo-checkout-1' -H 'Content-Type: application/json' -d '{
  "items": [{"sku": "SKU-HELIOS-1", "quantity": 2, "unit_amount": 2900}],
  "currency": "USD"
}')
echo "$CHECKOUT"

echo "== search (CDC / outbox → ES, may lag a second)"
sleep 4
curl -sS "$GW/v1/search/products?q=observability" -H "$AUTH"
echo
echo "open Jaeger at http://localhost:16686 and search service=orchestrator"
