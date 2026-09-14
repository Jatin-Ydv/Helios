# gRPC in Helios

## Use case

**Checkout saga internals** use gRPC. The public edge stays HTTP/JSON.

```
Client ──HTTP──► Gateway ──HTTP──► Orchestrator
                                      │
                                      ├── gRPC ► OrderService
                                      ├── gRPC ► InventoryService
                                      ├── gRPC ► PaymentService
                                      └── gRPC ► FulfillmentService
```

HTTP APIs on order/inventory/payment/fulfillment remain for debugging and direct calls. Orchestrator no longer uses those HTTP clients for the saga.

## Interview defense

> At the edge we keep HTTP for curl, browsers, and the demo. Between trusted mesh services on the hot checkout path we use gRPC: a versioned protobuf contract, smaller binary payloads, and typed RPCs that match the saga steps (`CreateOrder`, `Reserve`, `Authorize`, …). Auth is still the internal mesh token, carried as gRPC metadata (`x-helios-internal`, `x-tenant-id`, `x-user-id`).

**Why not gRPC everywhere?** Public clients and the gateway proxy are HTTP-first. Kafka remains the async event bus — gRPC does not replace outbox/events.

## Contract

- Proto: [`proto/helios/v1/checkout.proto`](../proto/helios/v1/checkout.proto)
- Generated Go: [`gen/helios/v1/`](../gen/helios/v1/)
- Helpers: [`platform/grpcx`](../platform/grpcx/)

Regenerate after proto edits:

```bash
export PATH="$HOME/.local/bin:$HOME/go/bin:$PATH"
protoc -I proto \
  --go_out=. --go_opt=module=github.com/helios-commerce/helios \
  --go-grpc_out=. --go-grpc_opt=module=github.com/helios-commerce/helios \
  proto/helios/v1/checkout.proto
```

## Config

| Env | Meaning |
|---|---|
| `GRPC_ADDR` | Listen address for workers (default `:9090`) |
| `ORDER_GRPC` | Orchestrator dial target (default `order:9090`) |
| `INVENTORY_GRPC` | … |
| `PAYMENT_GRPC` | … |
| `FULFILLMENT_GRPC` | … |
