# Helm packaging for Helios

See the chart README: [`deployments/helm/helios/README.md`](../deployments/helm/helios/README.md).

## Why Helm and Kustomize coexist

| Tool | Role in this repo |
|---|---|
| **Kustomize** | Original overlays; still valid via `kubectl apply -k deployments/k8s/minikube` |
| **Helm** | Parameterized release (`values-minikube.yaml`), hooks for `kafka-init`, interview-friendly packaging |

Pick one install path per cluster. Mixing apply-k and helm without `--take-ownership` causes ownership conflicts.

## Quick commands

```bash
make helm-lint
make helm-up
make helm-down
```
