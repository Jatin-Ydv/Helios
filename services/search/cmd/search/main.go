package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/helios-commerce/helios/platform/boot"
	"github.com/helios-commerce/helios/platform/events"
	"github.com/helios-commerce/helios/platform/httpx"
	"github.com/helios-commerce/helios/platform/kafka"
	"github.com/helios-commerce/helios/platform/resilience"
	"github.com/helios-commerce/helios/platform/shutdown"
	"github.com/helios-commerce/helios/platform/tenant"
)

const indexName = "helios-products"

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := boot.New(ctx, "search", false, false, true)
	if err != nil {
		slog.Error("boot", "err", err)
		os.Exit(1)
	}
	es, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{rt.Cfg.ElasticURL}})
	if err != nil {
		rt.Log.Error("elasticsearch client", "err", err)
		os.Exit(1)
	}
	rt.Health.Register("elasticsearch", func(ctx context.Context) error {
		res, err := es.Ping(es.Ping.WithContext(ctx))
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.IsError() {
			return errors.New(res.Status())
		}
		return nil
	})
	svc := &search{rt: rt, es: es, cb: resilience.NewBreaker("elasticsearch", 8, 15*time.Second, nil)}
	if err := svc.ensureIndex(ctx); err != nil {
		rt.Log.Warn("ensure index (ES may still be starting)", "err", err)
	}
	rt.Server.Router.Route("/v1/search", func(r httpx.Router) {
		r.Use(httpx.AuthFlexible(rt.JWT, rt.InternalToken()))
		r.Get("/products", svc.query)
	})
	cons := kafka.NewConsumer(rt.Cfg.KafkaBrokers, "helios.catalog.events", "search-indexer", "search", rt.Log, rt.Metrics, svc.onCatalog)
	go func() {
		if err := cons.Run(ctx); err != nil {
			rt.Log.Error("consumer", "err", err)
		}
	}()
	go serve(rt)
	shutdown.Run(rt.Log, rt.Cfg.ShutdownWait,
		shutdown.Hook{Name: "http", Fn: func(ctx context.Context) error { cancel(); return rt.Server.HTTP.Shutdown(ctx) }},
		shutdown.Hook{Name: "kafka-c", Fn: func(ctx context.Context) error { return cons.Close() }},
		shutdown.Hook{Name: "kafka-p", Fn: func(ctx context.Context) error { return rt.Pub.Close() }},
		shutdown.Hook{Name: "otel", Fn: rt.Obs.Shutdown},
	)
}

func serve(rt *boot.Runtime) {
	rt.Log.Info("listen", "addr", rt.Cfg.HTTPAddr)
	if err := rt.Server.HTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		rt.Log.Error("http", "err", err)
	}
}

type search struct {
	rt *boot.Runtime
	es *elasticsearch.Client
	cb *resilience.Breaker
}

func (s *search) ensureIndex(ctx context.Context) error {
	res, err := s.es.Indices.Exists([]string{indexName}, s.es.Indices.Exists.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == 200 {
		return nil
	}
	mapping := `{
	  "settings": {"number_of_shards": 1, "number_of_replicas": 0, "analysis": {
	    "analyzer": {"helios": {"type": "custom", "tokenizer": "standard", "filter": ["lowercase", "asciifolding"]}}
	  }},
	  "mappings": {"properties": {
	    "tenant_id": {"type": "keyword"},
	    "sku": {"type": "keyword"},
	    "name": {"type": "text", "analyzer": "helios", "fields": {"raw": {"type": "keyword"}}},
	    "description": {"type": "text", "analyzer": "helios"},
	    "amount": {"type": "long"},
	    "currency": {"type": "keyword"},
	    "active": {"type": "boolean"},
	    "suggest": {"type": "completion"}
	  }}
	}`
	cr, err := s.es.Indices.Create(indexName, s.es.Indices.Create.WithBody(strings.NewReader(mapping)), s.es.Indices.Create.WithContext(ctx))
	if err != nil {
		return err
	}
	defer cr.Body.Close()
	if cr.IsError() {
		return errors.New(cr.String())
	}
	return nil
}

func (s *search) onCatalog(ctx context.Context, env events.Envelope) error {
	var p events.ProductPayload
	if err := env.Decode(&p); err != nil {
		return err
	}
	if env.EventType == events.ProductDeleted || !p.Active {
		_, err := s.es.Delete(indexName, p.ProductID, s.es.Delete.WithContext(ctx))
		return err
	}
	doc := map[string]any{
		"tenant_id": env.TenantID, "sku": p.SKU, "name": p.Name, "description": p.Description,
		"amount": p.Amount, "currency": p.Currency, "active": p.Active,
		"suggest": map[string]any{"input": []string{p.Name, p.SKU}},
	}
	body, _ := json.Marshal(doc)
	return s.cb.Execute(func() error {
		res, err := s.es.Index(indexName, bytes.NewReader(body),
			s.es.Index.WithDocumentID(p.ProductID),
			s.es.Index.WithContext(ctx),
			s.es.Index.WithRefresh("false"),
		)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.IsError() {
			return errors.New(res.Status())
		}
		return nil
	})
}

func (s *search) query(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	q := r.URL.Query().Get("q")
	if q == "" {
		httpx.Problem(w, http.StatusBadRequest, "q required")
		return
	}
	query := map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter": []any{
					map[string]any{"term": map[string]any{"tenant_id": ident.TenantID}},
					map[string]any{"term": map[string]any{"active": true}},
				},
				"must": map[string]any{
					"multi_match": map[string]any{
						"query": q, "fields": []string{"name^3", "description", "sku"}, "fuzziness": "AUTO",
					},
				},
			},
		},
		"size": 20,
	}
	body, _ := json.Marshal(query)
	res, err := s.es.Search(
		s.es.Search.WithContext(r.Context()),
		s.es.Search.WithIndex(indexName),
		s.es.Search.WithBody(bytes.NewReader(body)),
	)
	if err != nil {
		httpx.Problem(w, http.StatusBadGateway, "search unavailable")
		return
	}
	defer res.Body.Close()
	if res.IsError() {
		httpx.Problem(w, http.StatusBadGateway, res.Status())
		return
	}
	var parsed map[string]any
	if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		httpx.Problem(w, http.StatusInternalServerError, "decode")
		return
	}
	httpx.JSON(w, http.StatusOK, parsed)
}
