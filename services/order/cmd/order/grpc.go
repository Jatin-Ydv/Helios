package main

import (
	"context"
	"errors"

	heliosv1 "github.com/helios-commerce/helios/gen/helios/v1"
	"github.com/helios-commerce/helios/platform/database"
	"github.com/helios-commerce/helios/platform/events"
	"github.com/helios-commerce/helios/platform/grpcx"
	"github.com/helios-commerce/helios/platform/id"
	"github.com/helios-commerce/helios/platform/tenant"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
)

type orderGRPC struct {
	heliosv1.UnimplementedOrderServiceServer
	svc *orders
}

func (g *orderGRPC) CreateOrder(ctx context.Context, req *heliosv1.CreateOrderRequest) (*heliosv1.CreateOrderResponse, error) {
	ident := tenant.MustFrom(ctx)
	if len(req.Items) == 0 {
		return nil, grpcx.Err(codes.InvalidArgument, "items required")
	}
	currency := req.Currency
	if currency == "" {
		currency = "USD"
	}
	customerID := req.CustomerId
	if customerID == "" {
		customerID = ident.UserID
	}
	items := make([]item, 0, len(req.Items))
	var total int64
	for _, it := range req.Items {
		if it.Quantity < 1 || it.Sku == "" {
			return nil, grpcx.Err(codes.InvalidArgument, "invalid item")
		}
		items = append(items, item{SKU: it.Sku, Quantity: it.Quantity, UnitAmount: it.UnitAmount})
		total += it.UnitAmount * it.Quantity
	}
	oid := id.NewULID()
	dto := orderDTO{ID: oid, CustomerID: customerID, Status: "pending", Amount: total, Currency: currency, Items: items, Version: 1}
	err := database.InTx(ctx, g.svc.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO orders (id, tenant_id, customer_id, status, amount, currency)
			VALUES ($1,$2,$3,'pending',$4,$5)`, oid, ident.TenantID, customerID, total, currency); err != nil {
			return err
		}
		for _, it := range items {
			if _, err := tx.Exec(ctx, `INSERT INTO order_items (order_id, sku, quantity, unit_amount) VALUES ($1,$2,$3,$4)`,
				oid, it.SKU, it.Quantity, it.UnitAmount); err != nil {
				return err
			}
		}
		return g.svc.emit(ctx, tx, events.OrderCreated, dto, "")
	})
	if err != nil {
		return nil, grpcx.Err(codes.Internal, err.Error())
	}
	return &heliosv1.CreateOrderResponse{
		OrderId:  dto.ID,
		Amount:   dto.Amount,
		Currency: dto.Currency,
		Status:   dto.Status,
	}, nil
}

func (g *orderGRPC) ConfirmOrder(ctx context.Context, req *heliosv1.OrderIDRequest) (*heliosv1.OrderActionResponse, error) {
	return g.transition(ctx, req.OrderId, "confirmed", events.OrderConfirmed)
}

func (g *orderGRPC) FailOrder(ctx context.Context, req *heliosv1.OrderIDRequest) (*heliosv1.OrderActionResponse, error) {
	return g.transition(ctx, req.OrderId, "failed", events.OrderFailed)
}

func (g *orderGRPC) transition(ctx context.Context, oid, to, eventType string) (*heliosv1.OrderActionResponse, error) {
	ident := tenant.MustFrom(ctx)
	if oid == "" {
		return nil, grpcx.Err(codes.InvalidArgument, "order_id required")
	}
	var dto orderDTO
	err := database.InTx(ctx, g.svc.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		dto, err = g.svc.loadTx(ctx, tx, ident.TenantID, oid)
		if err != nil {
			return err
		}
		if !legal(dto.Status, to) {
			return errIllegal
		}
		if _, err := tx.Exec(ctx, `UPDATE orders SET status=$1, version=version+1, updated_at=now() WHERE id=$2 AND tenant_id=$3`, to, oid, ident.TenantID); err != nil {
			return err
		}
		dto.Status = to
		dto.Version++
		return g.svc.emit(ctx, tx, eventType, dto, "")
	})
	if errors.Is(err, errIllegal) {
		return nil, grpcx.Err(codes.FailedPrecondition, "illegal transition")
	}
	if err != nil {
		return nil, grpcx.Err(codes.NotFound, "order")
	}
	return &heliosv1.OrderActionResponse{OrderId: dto.ID, Status: dto.Status}, nil
}
