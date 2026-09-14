package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/helios-commerce/helios/platform/boot"
	"github.com/helios-commerce/helios/platform/httpx"
	"github.com/helios-commerce/helios/platform/id"
	"github.com/helios-commerce/helios/platform/shutdown"
	"github.com/helios-commerce/helios/platform/tenant"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

func main() {
	ctx := context.Background()
	rt, err := boot.New(ctx, "identity", true, true, false)
	if err != nil {
		slog.Error("boot", "err", err)
		os.Exit(1)
	}
	svc := &service{pool: rt.Pool, rt: rt}
	r := rt.Server.Router
	r.Route("/v1/auth", func(r httpx.Router) {
		r.Post("/register", svc.register)
		r.Post("/login", svc.login)
		r.Post("/refresh", svc.refresh)
		r.Group(func(r httpx.Router) {
			r.Use(httpx.Auth(rt.JWT))
			r.Get("/me", svc.me)
		})
	})
	go func() {
		rt.Log.Info("listen", "addr", rt.Cfg.HTTPAddr)
		if err := rt.Server.HTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			rt.Log.Error("http", "err", err)
		}
	}()
	shutdown.Run(rt.Log, rt.Cfg.ShutdownWait,
		shutdown.Hook{Name: "http", Fn: func(ctx context.Context) error { return rt.Server.HTTP.Shutdown(ctx) }},
		shutdown.Hook{Name: "db", Fn: func(ctx context.Context) error { rt.Pool.Close(); return nil }},
		shutdown.Hook{Name: "otel", Fn: rt.Obs.Shutdown},
	)
}

type service struct {
	pool *pgxpool.Pool
	rt   *boot.Runtime
}

type registerReq struct {
	TenantName string `json:"tenant_name"`
	TenantID   string `json:"tenant_id"`
	Email      string `json:"email"`
	Password   string `json:"password"`
}

func (s *service) register(w http.ResponseWriter, r *http.Request) {
	var req registerReq
	if err := httpx.Decode(r, &req); err != nil {
		httpx.Problem(w, http.StatusBadRequest, "invalid json")
		return
	}
	req.Email = strings.ToLower(strings.TrimSpace(req.Email))
	if req.Email == "" || len(req.Password) < 10 {
		httpx.Problem(w, http.StatusBadRequest, "email required, password min 10 chars")
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		httpx.Problem(w, http.StatusInternalServerError, "hash failed")
		return
	}
	tid := req.TenantID
	if tid == "" {
		tid = id.NewULID()
	}
	uid := id.NewULID()
	ctx := r.Context()
	err = pgxTx(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO tenants (id, name) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING`, tid, coalesce(req.TenantName, "default")); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO users (id, tenant_id, email, password_hash, role) VALUES ($1,$2,$3,$4,'admin')`, uid, tid, req.Email, string(hash))
		return err
	})
	if err != nil {
		httpx.Problem(w, http.StatusConflict, "email already registered for tenant")
		return
	}
	tokens, err := s.issue(ctx, tid, uid, "admin")
	if err != nil {
		httpx.Problem(w, http.StatusInternalServerError, "token issue")
		return
	}
	httpx.JSON(w, http.StatusCreated, map[string]any{"tenant_id": tid, "user_id": uid, "tokens": tokens})
}

type loginReq struct {
	TenantID string `json:"tenant_id"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

func (s *service) login(w http.ResponseWriter, r *http.Request) {
	var req loginReq
	if err := httpx.Decode(r, &req); err != nil {
		httpx.Problem(w, http.StatusBadRequest, "invalid json")
		return
	}
	var uid, hash, role, tid string
	err := s.pool.QueryRow(r.Context(), `
		SELECT id, password_hash, role, tenant_id FROM users
		WHERE email=$1 AND ($2 = '' OR tenant_id=$2)
	`, strings.ToLower(req.Email), req.TenantID).Scan(&uid, &hash, &role, &tid)
	if err != nil || bcrypt.CompareHashAndPassword([]byte(hash), []byte(req.Password)) != nil {
		httpx.Problem(w, http.StatusUnauthorized, "invalid credentials")
		return
	}
	tokens, err := s.issue(r.Context(), tid, uid, role)
	if err != nil {
		httpx.Problem(w, http.StatusInternalServerError, "token issue")
		return
	}
	httpx.JSON(w, http.StatusOK, tokens)
}

type refreshReq struct {
	RefreshToken string `json:"refresh_token"`
}

func (s *service) refresh(w http.ResponseWriter, r *http.Request) {
	var req refreshReq
	if err := httpx.Decode(r, &req); err != nil {
		httpx.Problem(w, http.StatusBadRequest, "invalid json")
		return
	}
	var uid, tid, role string
	err := s.pool.QueryRow(r.Context(), `
		SELECT u.id, u.tenant_id, u.role
		FROM refresh_tokens t JOIN users u ON u.id = t.user_id
		WHERE t.token=$1 AND t.revoked_at IS NULL AND t.expires_at > now()
	`, req.RefreshToken).Scan(&uid, &tid, &role)
	if err != nil {
		httpx.Problem(w, http.StatusUnauthorized, "invalid refresh")
		return
	}
	_, _ = s.pool.Exec(r.Context(), `UPDATE refresh_tokens SET revoked_at=now() WHERE token=$1`, req.RefreshToken)
	tokens, err := s.issue(r.Context(), tid, uid, role)
	if err != nil {
		httpx.Problem(w, http.StatusInternalServerError, "token issue")
		return
	}
	httpx.JSON(w, http.StatusOK, tokens)
}

func (s *service) me(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	var email, role string
	if err := s.pool.QueryRow(r.Context(), `SELECT email, role FROM users WHERE id=$1 AND tenant_id=$2`, ident.UserID, ident.TenantID).Scan(&email, &role); err != nil {
		httpx.Problem(w, http.StatusNotFound, "user")
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"user_id": ident.UserID, "tenant_id": ident.TenantID, "email": email, "role": role})
}

func (s *service) issue(ctx context.Context, tid, uid, role string) (any, error) {
	tok, err := s.rt.JWT.Issue(tid, uid, role)
	if err != nil {
		return nil, err
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO refresh_tokens (token, user_id, tenant_id, expires_at) VALUES ($1,$2,$3,$4)`,
		tok.RefreshToken, uid, tid, time.Now().Add(30*24*time.Hour))
	if err != nil {
		return nil, err
	}
	_ = s.rt.Redis.Set(ctx, "session:"+uid, tid, 15*time.Minute).Err()
	return tok, nil
}

func pgxTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func coalesce(v, d string) string {
	if v == "" {
		return d
	}
	return v
}
