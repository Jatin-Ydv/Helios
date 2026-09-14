// Package config loads twelve-factor configuration from the environment with
// explicit defaults. No Viper, no magic files in production — ConfigMaps and
// Secrets become env vars at the kubelet. Locally, docker-compose does the same.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName    string
	HTTPAddr       string
	LogLevel       string
	Environment    string
	PostgresDSN    string
	RedisAddr      string
	RedisPassword  string
	KafkaBrokers   []string
	OTLPEndpoint   string
	JWTSecret      string
	JWTIssuer      string
	ElasticURL         string
	AerospikeHosts     string
	AerospikeNamespace string
	GRPCAddr           string
	ShutdownWait       time.Duration
	OutboxPoll         time.Duration
	SagaTimeout        time.Duration
}

func Load(service string) Config {
	return Config{
		ServiceName:   service,
		HTTPAddr:      env("HTTP_ADDR", ":8080"),
		LogLevel:      env("LOG_LEVEL", "info"),
		Environment:   env("ENVIRONMENT", "local"),
		PostgresDSN:   env("POSTGRES_DSN", ""),
		RedisAddr:     env("REDIS_ADDR", "localhost:6379"),
		RedisPassword: env("REDIS_PASSWORD", ""),
		KafkaBrokers:  split(env("KAFKA_BROKERS", "localhost:9092")),
		OTLPEndpoint:  env("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4318"),
		JWTSecret:     env("JWT_SECRET", "helios-dev-only-change-me"),
		JWTIssuer:     env("JWT_ISSUER", "helios-identity"),
		ElasticURL:         env("ELASTICSEARCH_URL", "http://localhost:9200"),
		AerospikeHosts:     env("AEROSPIKE_HOSTS", "localhost:3000"),
		AerospikeNamespace: env("AEROSPIKE_NAMESPACE", "helios"),
		GRPCAddr:           env("GRPC_ADDR", ":9090"),
		ShutdownWait:       envDur("SHUTDOWN_TIMEOUT", 20*time.Second),
		OutboxPoll:         envDur("OUTBOX_POLL_INTERVAL", 400*time.Millisecond),
		SagaTimeout:        envDur("SAGA_TIMEOUT", 30*time.Second),
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}

func EnvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

func split(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (c Config) MustValidate() {
	if c.JWTSecret == "helios-dev-only-change-me" && c.Environment == "prod" {
		panic(fmt.Sprintf("%s: JWT_SECRET must be set in production", c.ServiceName))
	}
}
