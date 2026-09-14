package main

import (
	"context"
	"errors"

	heliosv1 "github.com/helios-commerce/helios/gen/helios/v1"
	"github.com/helios-commerce/helios/platform/database"
	"github.com/helios-commerce/helios/platform/events"
	"github.com/helios-commerce/helios/platform/grpcx"
	"github.com/helios-commerce/helios/platform/id"
	"github.com/helios-commerce/helios/platform/outbox"
	"github.com/helios-commerce/helios/platform/resilience"
	"github.com/helios-commerce/helios/platform/tenant"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
)

type paymentGRPC struct {
	heliosv1.UnimplementedPaymentServiceServer
	svc *payments
}

func (g *paymentGRPC) Authorize(ctx context.Context, req *heliosv1.AuthorizeRequest) (*heliosv1.AuthorizeResponse, error) {
	ident := tenant.MustFrom(ctx)
	if req.OrderId == "" || req.Amount < 1 {
		return nil, grpcx.Err(codes.InvalidArgument, "order_id and amount required")
	}
	currency := req.Currency
	if currency == "" {
		currency = "USD"
	}
	pid := id.NewULID()
	var pspRef string
	err := g.svc.bh.Execute(ctx, func() error {
		return g.svc.cb.Execute(func() error {
			return resilience.Do(ctx, resilience.DefaultRetry(), func(int) error {
				var e error
				pspRef, e = g.svc.psp.Authorize(ctx, req.OrderId, req.Amount)
				return e
			})
		})
	})
	if errors.Is(err, resilience.ErrOpen) || errors.Is(err, resilience.ErrBulkheadFull) {
		return nil, grpcx.Err(codes.Unavailable, "payment dependency unavailable")
	}
	status := "authorized"
	ev := events.PaymentAuthorized
	reason := ""
	if err != nil {
		status = "failed"
		ev = events.PaymentAuthorizationFailed
		reason = err.Error()
	}
	err = database.InTx(ctx, g.svc.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO payments (id, tenant_id, order_id, amount, currency, status, psp_reference)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
			ON CONFLICT (tenant_id, order_id) DO NOTHING`,
			pid, ident.TenantID, req.OrderId, req.Amount, currency, status, pspRef); err != nil {
			return err
		}
		env, err := events.New(ctx, ev, "payment", "payment", pid, events.PaymentPayload{
			PaymentID: pid, OrderID: req.OrderId, Amount: req.Amount, Currency: currency, Status: status, PSPReference: pspRef, Reason: reason,
		})
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, env)
	})
	if err != nil {
		return nil, grpcx.Err(codes.Internal, err.Error())
	}
	resp := &heliosv1.AuthorizeResponse{PaymentId: pid, Status: status, PspReference: pspRef, Reason: reason}
	return resp, nil
}

func (g *paymentGRPC) Void(ctx context.Context, req *heliosv1.VoidRequest) (*heliosv1.VoidResponse, error) {
	ident := tenant.MustFrom(ctx)
	if req.PaymentId == "" {
		return nil, grpcx.Err(codes.InvalidArgument, "payment_id required")
	}
	var orderID string
	err := database.InTx(ctx, g.svc.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			UPDATE payments SET status='voided'
			WHERE id=$1 AND tenant_id=$2 AND status='authorized'
			RETURNING order_id`, req.PaymentId, ident.TenantID).Scan(&orderID)
		if err != nil {
			return err
		}
		env, err := events.New(ctx, events.PaymentVoided, "payment", "payment", req.PaymentId, events.PaymentPayload{
			PaymentID: req.PaymentId, OrderID: orderID, Status: "voided",
		})
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, env)
	})
	if err != nil {
		return nil, grpcx.Err(codes.FailedPrecondition, "not voidable")
	}
	return &heliosv1.VoidResponse{PaymentId: req.PaymentId, Status: "voided"}, nil
}
