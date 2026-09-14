# syntax=docker/dockerfile:1.7
FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG SERVICE
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/service ./services/${SERVICE}/cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
ARG SERVICE
COPY --from=build /out/service /app/service
COPY services/${SERVICE}/migrations /app/migrations
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/app/service"]
