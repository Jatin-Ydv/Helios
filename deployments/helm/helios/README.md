# Helios with Helm

Helios can be deployed with **Helm** or **Kustomize**. Same images and runtime behavior; Helm adds templating and per-environment `values` files.

## Interview one-liner

> We package Helios as a Helm chart so infra and the ten Go services share one release, with `values-minikube.yaml` for laptop sizing and a path to swap values for staging/prod without forking YAML.

## Chart layout

```
deployments/helm/helios/
  Chart.yaml
  values.yaml              # defaults
  values-minikube.yaml     # local profile (apps, waits, gRPC ports)
  configs/aerospike.conf
  files/00-init.sh
  files/kafka-init.sh
  templates/               # namespace, config, infra, apps
```

Kustomize remains at `deployments/k8s/minikube/` for `kubectl apply -k` workflows.

## Prerequisites

1. minikube running (`minikube start` or `make minikube-up` for images)
2. Helios images loaded (`helios/*:minikube`) — `make minikube-up` or load manually
3. Helm 3 on `PATH` (`~/.local/bin/helm`)

## Install / upgrade

```bash
export PATH="$HOME/.local/bin:$PATH"

helm upgrade --install helios ./deployments/helm/helios \
  --namespace helios \
  --create-namespace \
  -f ./deployments/helm/helios/values-minikube.yaml \
  --take-ownership
```

`--take-ownership` lets Helm adopt resources previously applied with Kustomize (same names).

Or via Make:

```bash
make helm-up      # upgrade --install with values-minikube
make helm-lint    # lint + template dry-run
make helm-down    # uninstall release (leaves PVCs/emptyDirs as applicable)
```

## Values you will change

| Key | Purpose |
|---|---|
| `imageTag` | e.g. `minikube` or a CI sha |
| `apps.*.replicas` | scale a service |
| `apps.*.resources` | memory/CPU |
| `secrets.JWT_SECRET` | never commit real prod secrets; use `--set` / Sealed Secrets / External Secrets in real envs |
| `debezium.enabled` | Deploy Kafka Connect + register outbox connectors (catalog/order) |
| `apps.catalog.env.OUTBOX_RELAY_ENABLED` | Must be `"false"` when Debezium owns catalog publish |
| `kafka.heapOpts` / `elasticsearch.javaOpts` | laptop vs server heaps |

## Lint locally

```bash
helm lint ./deployments/helm/helios -f ./deployments/helm/helios/values-minikube.yaml
helm template helios ./deployments/helm/helios -f ./deployments/helm/helios/values-minikube.yaml | head
```
