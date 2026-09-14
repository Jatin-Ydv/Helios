package metrics

import "github.com/prometheus/client_golang/prometheus"

type ServiceMetrics struct {
	HTTPRequests   *prometheus.CounterVec
	HTTPDuration   *prometheus.HistogramVec
	KafkaPublished *prometheus.CounterVec
	KafkaConsumed  *prometheus.CounterVec
	KafkaDLQ       *prometheus.CounterVec
	OutboxLag      prometheus.Gauge
	SagaActive     prometheus.Gauge
	SagaFailed     prometheus.Counter
	CBState        *prometheus.GaugeVec
	CacheHit       *prometheus.CounterVec
}

func New(reg prometheus.Registerer, service string) *ServiceMetrics {
	m := &ServiceMetrics{
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "helios", Subsystem: service, Name: "http_requests_total",
			Help: "HTTP requests partitioned by route, method, and code.",
		}, []string{"method", "route", "code"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "helios", Subsystem: service, Name: "http_request_duration_seconds",
			Help:    "HTTP handler latency.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"method", "route"}),
		KafkaPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "helios", Subsystem: service, Name: "kafka_published_total",
			Help: "Events published to Kafka.",
		}, []string{"topic", "result"}),
		KafkaConsumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "helios", Subsystem: service, Name: "kafka_consumed_total",
			Help: "Events consumed from Kafka.",
		}, []string{"topic", "result"}),
		KafkaDLQ: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "helios", Subsystem: service, Name: "kafka_dlq_total",
			Help: "Events routed to a dead-letter topic after exhausted retries.",
		}, []string{"topic"}),
		OutboxLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "helios", Subsystem: service, Name: "outbox_unpublished_count",
			Help: "Unpublished outbox rows. SLO: < 100 under steady state.",
		}),
		SagaActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "helios", Subsystem: service, Name: "saga_active",
			Help: "In-flight checkout sagas.",
		}),
		SagaFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "helios", Subsystem: service, Name: "saga_failed_total",
			Help: "Sagas that entered a compensated or failed terminal state.",
		}),
		CBState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "helios", Subsystem: service, Name: "circuit_breaker_state",
			Help: "0=closed 1=half-open 2=open",
		}, []string{"name"}),
		CacheHit: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "helios", Subsystem: service, Name: "cache_requests_total",
			Help: "Cache hits and misses.",
		}, []string{"cache", "result"}),
	}
	reg.MustRegister(m.HTTPRequests, m.HTTPDuration, m.KafkaPublished, m.KafkaConsumed,
		m.KafkaDLQ, m.OutboxLag, m.SagaActive, m.SagaFailed, m.CBState, m.CacheHit)
	return m
}
