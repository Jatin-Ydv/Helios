SHELL := /bin/bash
export PATH := $(HOME)/.local/bin:$(PATH)
COMPOSE := docker compose
SERVICES := identity catalog inventory order payment fulfillment orchestrator search notification gateway

.PHONY: tidy test test-race up down logs connectors demo seed fmt vet build minikube-up minikube-down minikube-demo helm-lint helm-up helm-down

HELM := $(HOME)/.local/bin/helm
HELM_CHART := deployments/helm/helios
HELM_VALUES := deployments/helm/helios/values-minikube.yaml

tidy:
	go mod tidy

fmt:
	gofmt -w platform services

vet:
	go vet ./...

test:
	go test ./...

test-race:
	go test -race ./...

build:
	@for s in $(SERVICES); do \
	  echo "==> $$s"; \
	  CGO_ENABLED=0 go build -o bin/$$s ./services/$$s/cmd/$$s; \
	done

up:
	$(COMPOSE) up -d --build
	@echo "Gateway  http://localhost:8080"
	@echo "Jaeger   http://localhost:16686"
	@echo "Grafana  http://localhost:3000 (admin/admin)"
	@echo "Kafka UI http://localhost:8081"
	@echo "Connect  http://localhost:8083"

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=100

connectors:
	bash scripts/register-connectors.sh

demo:
	bash scripts/demo.sh

seed: demo

minikube-up:
	bash scripts/minikube-up.sh

minikube-down:
	minikube delete

minikube-demo:
	@echo "port-forward gateway 8080 (WSL cannot reach minikube NodePort directly)"
	@kubectl -n helios port-forward svc/gateway 8080:8080 >/tmp/helios-gw-pf.log 2>&1 & echo $$! > /tmp/helios-gw-pf.pid
	@sleep 1
	GATEWAY_URL=http://127.0.0.1:8080 bash scripts/demo.sh

helm-lint:
	$(HELM) lint $(HELM_CHART) -f $(HELM_VALUES)
	$(HELM) template helios $(HELM_CHART) -f $(HELM_VALUES) >/dev/null
	@echo "helm lint + template OK"

helm-up:
	$(HELM) upgrade --install helios $(HELM_CHART) \
	  --namespace helios --create-namespace \
	  -f $(HELM_VALUES) \
	  --take-ownership \
	  --wait --timeout 10m
	@echo "Helm release helios installed. Port-forward: kubectl -n helios port-forward svc/gateway 18080:8080"

helm-down:
	$(HELM) uninstall helios --namespace helios || true
