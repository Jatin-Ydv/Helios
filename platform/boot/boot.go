package boot

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/helios-commerce/helios/platform/auth"
	"github.com/helios-commerce/helios/platform/config"
	"github.com/helios-commerce/helios/platform/database"
	"github.com/helios-commerce/helios/platform/health"
	"github.com/helios-commerce/helios/platform/httpx"
	"github.com/helios-commerce/helios/platform/kafka"
	"github.com/helios-commerce/helios/platform/metrics"
	"github.com/helios-commerce/helios/platform/observability"
	"github.com/helios-commerce/helios/platform/outbox"
	"github.com/helios-commerce/helios/platform/redisx"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Runtime struct {
	Cfg     config.Config
	Obs     *observability.Handle
	Metrics *metrics.ServiceMetrics
	Health  *health.Checker
	Pool    *pgxpool.Pool
	Redis   *redis.Client
	JWT     *auth.Service
	Pub     *kafka.Producer
	Server  *httpx.Server
	Log     *slog.Logger
}

func New(ctx context.Context, service string, withDB, withRedis, withKafka bool) (*Runtime, error) {
	cfg := config.Load(service)
	obs, err := observability.Setup(ctx, service, cfg.Environment, cfg.OTLPEndpoint, cfg.LogLevel)
	if err != nil {
		return nil, err
	}
	m := metrics.New(obs.Registry, service)
	hc := health.New()
	rt := &Runtime{
		Cfg:     cfg,
		Obs:     obs,
		Metrics: m,
		Health:  hc,
		JWT:     auth.New(cfg.JWTSecret, cfg.JWTIssuer),
		Log:     obs.Logger,
		Server:  httpx.NewServer(cfg.HTTPAddr, obs, m, hc),
	}
	if withDB {
		pool, err := database.Connect(ctx, cfg.PostgresDSN)
		if err != nil {
			return nil, err
		}
		rt.Pool = pool
		hc.Register("postgres", func(ctx context.Context) error { return database.Ping(ctx, pool) })
		if dir := migrateDir(service); dir != "" {
			if err := database.Migrate(ctx, pool, dir); err != nil {
				return nil, err
			}
		}
	}
	if withRedis {
		rt.Redis = redisx.Connect(cfg.RedisAddr, cfg.RedisPassword)
		hc.Register("redis", func(ctx context.Context) error { return redisx.Ping(ctx, rt.Redis) })
	}
	if withKafka {
		rt.Pub = kafka.NewProducer(cfg.KafkaBrokers, service, obs.Logger, m)
	}
	return rt, nil
}

func (rt *Runtime) StartOutboxRelay(ctx context.Context) {
	if rt.Pool == nil || rt.Pub == nil {
		return
	}
	relay := &outbox.Relay{
		Pool:     rt.Pool,
		Pub:      rt.Pub,
		Resolve:  outbox.DefaultTopics,
		Log:      rt.Log,
		Interval: rt.Cfg.OutboxPoll,
		OnLag: func(n int64) {
			rt.Metrics.OutboxLag.Set(float64(n))
		},
	}
	go relay.Run(ctx)
}

func (rt *Runtime) InternalToken() string {
	if v := os.Getenv("INTERNAL_TOKEN"); v != "" {
		return v
	}
	return "helios-internal-dev"
}

func migrateDir(service string) string {
	candidates := []string{
		"migrations",
		"/app/migrations",
		filepath.Join("services", service, "migrations"),
	}
	if exe, err := os.Executable(); err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(exe), "migrations"))
	}
	for _, c := range candidates {
		if st, err := os.Stat(c); err == nil && st.IsDir() {
			matches, _ := filepath.Glob(filepath.Join(c, "*.sql"))
			if len(matches) > 0 || strings.HasPrefix(c, "/app") {
				return c
			}
		}
	}
	return ""
}
