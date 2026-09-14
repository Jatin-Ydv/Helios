#!/usr/bin/env bash
# Build Helios images inside minikube's Docker and deploy the minikube overlay.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export PATH="$HOME/.local/bin:/usr/local/bin:$PATH"

SERVICES=(identity catalog inventory order payment fulfillment orchestrator search notification gateway)

if ! command -v minikube >/dev/null; then
  echo "minikube not on PATH" >&2
  exit 1
fi

if ! minikube status >/dev/null 2>&1; then
  echo "==> starting minikube (docker driver, 4.5Gi)"
  minikube start --driver=docker --cpus=4 --memory=4500 --disk-size=20g
fi

echo "==> building images on host Docker, then loading into minikube"
unset DOCKER_TLS_VERIFY DOCKER_HOST DOCKER_CERT_PATH MINIKUBE_ACTIVE_DOCKERD || true
export DOCKER_BUILDKIT=1

for s in "${SERVICES[@]}"; do
  echo "==> build helios/${s}:minikube"
  docker build -f deployments/docker/Dockerfile --build-arg SERVICE="$s" -t "helios/${s}:minikube" .
  echo "==> minikube image load helios/${s}:minikube"
  minikube image load "helios/${s}:minikube"
done

echo "==> kubectl apply -k deployments/k8s/minikube"
kubectl apply -k deployments/k8s/minikube

echo "==> waiting for core infra"
kubectl -n helios rollout status deploy/postgres --timeout=180s
kubectl -n helios rollout status deploy/redis --timeout=180s
kubectl -n helios rollout status deploy/kafka --timeout=240s
kubectl -n helios rollout status deploy/elasticsearch --timeout=300s
kubectl -n helios rollout status deploy/jaeger --timeout=180s

echo "==> waiting for kafka-init job"
kubectl -n helios wait --for=condition=complete job/kafka-init --timeout=180s

echo "==> waiting for apps"
for s in "${SERVICES[@]}"; do
  kubectl -n helios rollout status "deploy/${s}" --timeout=240s
done

echo
echo "All Helios pods should be Ready. Docker Desktop + WSL cannot reach NodePorts on"
echo "the minikube IP, so tunnel with:"
echo
echo "  kubectl -n helios port-forward svc/gateway 8080:8080"
echo "  kubectl -n helios port-forward svc/jaeger 16686:16686"
echo
echo "Then:  make demo          # GATEWAY_URL defaults to http://localhost:8080"
echo "        make minikube-demo"
echo "        make minikube-down"
