package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/helios-commerce/helios/platform/events"
	"github.com/helios-commerce/helios/platform/metrics"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Producer struct {
	w    *kafkago.Writer
	log  *slog.Logger
	m    *metrics.ServiceMetrics
	name string
}

func NewProducer(brokers []string, clientID string, log *slog.Logger, m *metrics.ServiceMetrics) *Producer {
	return &Producer{
		name: clientID,
		log:  log,
		m:    m,
		w: &kafkago.Writer{
			Addr:                   kafkago.TCP(brokers...),
			Balancer:               &kafkago.Hash{}, // key = aggregate id → partition stickiness
			RequiredAcks:           kafkago.RequireAll,
			Async:                  false,
			AllowAutoTopicCreation: false,
			BatchTimeout:           10 * time.Millisecond,
			WriteTimeout:           5 * time.Second,
			MaxAttempts:            8,
		},
	}
}

func (p *Producer) Publish(ctx context.Context, topic string, env events.Envelope) error {
	ctx, span := otel.Tracer(p.name).Start(ctx, "kafka.publish",
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", topic),
			attribute.String("messaging.operation", "publish"),
			attribute.String("helios.event_type", env.EventType),
			attribute.String("helios.aggregate_id", env.AggregateID),
		),
	)
	defer span.End()

	body, err := env.Bytes()
	if err != nil {
		span.RecordError(err)
		return err
	}
	headers := []kafkago.Header{
		{Key: "event_type", Value: []byte(env.EventType)},
		{Key: "event_id", Value: []byte(env.EventID)},
		{Key: "tenant_id", Value: []byte(env.TenantID)},
		{Key: "correlation_id", Value: []byte(env.CorrelationID)},
		{Key: "producer", Value: []byte(env.Producer)},
	}
	for k, v := range env.Trace {
		headers = append(headers, kafkago.Header{Key: k, Value: []byte(v)})
	}
	err = p.w.WriteMessages(ctx, kafkago.Message{
		Topic:   topic,
		Key:     []byte(env.AggregateID),
		Value:   body,
		Headers: headers,
		Time:    env.OccurredAt,
	})
	result := "ok"
	if err != nil {
		result = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	if p.m != nil {
		p.m.KafkaPublished.WithLabelValues(topic, result).Inc()
	}
	return err
}

func (p *Producer) Close() error { return p.w.Close() }

type Handler func(ctx context.Context, env events.Envelope) error

type Consumer struct {
	r      *kafkago.Reader
	dlq    *kafkago.Writer
	log    *slog.Logger
	m      *metrics.ServiceMetrics
	name   string
	topic  string
	handle Handler
	maxTry int
}

func NewConsumer(brokers []string, topic, group, clientID string, log *slog.Logger, m *metrics.ServiceMetrics, h Handler) *Consumer {
	return &Consumer{
		name:   clientID,
		topic:  topic,
		handle: h,
		log:    log,
		m:      m,
		maxTry: 5,
		r: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers:        brokers,
			GroupID:        group,
			Topic:          topic,
			MinBytes:       1,
			MaxBytes:       10e6,
			MaxWait:        500 * time.Millisecond,
			CommitInterval: 0, // manual commit after successful handler
			StartOffset:    kafkago.LastOffset,
		}),
		dlq: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Topic:        topic + ".dlq",
			RequiredAcks: kafkago.RequireAll,
			Balancer:     &kafkago.Hash{},
		},
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.log.Error("kafka fetch", "err", err)
			continue
		}
		if err := c.process(ctx, msg); err != nil {
			c.log.Error("kafka process", "offset", msg.Offset, "err", err)
			continue
		}
		if err := c.r.CommitMessages(ctx, msg); err != nil {
			c.log.Error("kafka commit", "err", err)
		}
	}
}

func (c *Consumer) process(ctx context.Context, msg kafkago.Message) error {
	env, err := events.Parse(msg.Value)
	if err != nil {
		return c.deadLetter(ctx, msg, fmt.Errorf("parse: %w", err))
	}
	ctx = events.ExtractTrace(ctx, env)
	ctx = events.WithCorrelation(ctx, env.CorrelationID)
	ctx = events.WithCausation(ctx, env.EventID)

	ctx, span := otel.Tracer(c.name).Start(ctx, "kafka.consume",
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.source", c.topic),
			attribute.String("helios.event_type", env.EventType),
			attribute.String("helios.event_id", env.EventID),
		),
	)
	defer span.End()

	for attempt := 1; attempt <= c.maxTry; attempt++ {
		err = c.handle(ctx, env)
		if err == nil {
			break
		}
		span.RecordError(err)
		if attempt < c.maxTry {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt*attempt) * 50 * time.Millisecond):
			}
		}
	}
	result := "ok"
	if err != nil {
		result = "error"
		if dlqErr := c.deadLetter(ctx, msg, err); dlqErr != nil {
			return dlqErr
		}
	}
	if c.m != nil {
		c.m.KafkaConsumed.WithLabelValues(c.topic, result).Inc()
	}
	return nil
}

func (c *Consumer) deadLetter(ctx context.Context, msg kafkago.Message, cause error) error {
	if c.m != nil {
		c.m.KafkaDLQ.WithLabelValues(c.topic).Inc()
	}
	c.log.Error("routing to dlq", "topic", c.topic, "offset", msg.Offset, "err", cause)
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers, kafkago.Header{Key: "dlq_reason", Value: []byte(cause.Error())})
	return c.dlq.WriteMessages(ctx, kafkago.Message{
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	})
}

func (c *Consumer) Close() error {
	_ = c.dlq.Close()
	return c.r.Close()
}
