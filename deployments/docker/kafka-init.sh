#!/usr/bin/env bash
set -euo pipefail
BROKER="${KAFKA_BROKER:-kafka:29092}"
echo "waiting for kafka at $BROKER"
for i in $(seq 1 60); do
  if /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

topics=(
  helios.catalog.events
  helios.catalog.events.dlq
  helios.inventory.events
  helios.inventory.events.dlq
  helios.order.events
  helios.order.events.dlq
  helios.payment.events
  helios.payment.events.dlq
  helios.fulfillment.events
  helios.fulfillment.events.dlq
  helios.notification.commands
  helios.notification.commands.dlq
  helios.saga.events
  helios.saga.events.dlq
  helios.unknown.events
  helios.cdc.catalog.outbox
)

for t in "${topics[@]}"; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" --create --if-not-exists \
    --topic "$t" --partitions 6 --replication-factor 1
done
echo "topics ready"
