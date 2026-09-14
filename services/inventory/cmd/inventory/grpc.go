package main

import (
	"context"
	"errors"
	"time"

	heliosv1 "github.com/helios-commerce/helios/gen/helios/v1"
	"github.com/helios-commerce/helios/platform/database"
	"github.com/helios-commerce/helios/platform/events"
	"github.com/helios-commerce/helios/platform/grpcx"
	"github.com/helios-commerce/helios/platform/id"
	"github.com/helios-commerce/helios/platform/outbox"
	"github.com/helios-commerce/helios/platform/redisx"
	"github.com/helios-commerce/helios/platform/tenant"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
)

type inventoryGRPC struct {
	heliosv1.UnimplementedInventoryServiceServer
	svc *inventory
}

func (g *inventoryGRPC) Reserve(ctx context.Context, req *heliosv1.ReserveRequest) (*heliosv1.ReserveResponse, error) {
	ident := tenant.MustFrom(ctx)
	if req.Quantity < 1 || req.Sku == "" || req.OrderId == "" {
		return nil, grpcx.Err(codes.InvalidArgument, "order_id, sku, quantity required")
	}
	wh := req.WarehouseId
	if wh == "" {
		wh = "wh-default"
	}
	lockKey := "inv:" + ident.TenantID + ":" + wh + ":" + req.Sku
	lock, err := redisx.Acquire(ctx, g.svc.rt.Redis, lockKey, 3*time.Second)
	if err != nil {
		return nil, grpcx.Err(codes.Aborted, "reservation lock busy")
	}
	defer func() { _ = lock.Unlock(context.Background()) }()

	fence, err := redisx.Fence(ctx, g.svc.rt.Redis, lockKey)
	if err != nil {
		return nil, grpcx.Err(codes.Internal, "fence")
	}
	resID := id.NewULID()
	err = database.InTx(ctx, g.svc.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var onHand, reserved int64
		if err := tx.QueryRow(ctx, `
			SELECT on_hand, reserved FROM stock
			WHERE tenant_id=$1 AND warehouse_id=$2 AND sku=$3
			FOR UPDATE`, ident.TenantID, wh, req.Sku).Scan(&onHand, &reserved); err != nil {
			return errStock
		}
		if onHand-reserved < req.Quantity {
			env, err := events.New(ctx, events.InventoryReservationFailed, "inventory", "reservation", resID, events.InventoryReservePayload{
				ReservationID: resID, OrderID: req.OrderId, SKU: req.Sku, WarehouseID: wh, Quantity: req.Quantity, Reason: "insufficient_stock",
			})
			if err != nil {
				return err
			}
			_ = outbox.Insert(ctx, tx, env)
			return errInsufficient
		}
		if _, err := tx.Exec(ctx, `
			UPDATE stock SET reserved = reserved + $1, version = version + 1
			WHERE tenant_id=$2 AND warehouse_id=$3 AND sku=$4`,
			req.Quantity, ident.TenantID, wh, req.Sku); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO reservations (id, tenant_id, order_id, warehouse_id, sku, quantity, status, fence, expires_at)
			VALUES ($1,$2,$3,$4,$5,$6,'pending',$7, now() + interval '15 minutes')
			ON CONFLICT (tenant_id, order_id, sku) DO NOTHING`,
			resID, ident.TenantID, req.OrderId, wh, req.Sku, req.Quantity, fence); err != nil {
			return err
		}
		env, err := events.New(ctx, events.InventoryReserved, "inventory", "reservation", resID, events.InventoryReservePayload{
			ReservationID: resID, OrderID: req.OrderId, SKU: req.Sku, WarehouseID: wh, Quantity: req.Quantity,
		})
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, env)
	})
	switch {
	case errors.Is(err, errInsufficient):
		return nil, grpcx.Err(codes.FailedPrecondition, "insufficient stock")
	case errors.Is(err, errStock):
		return nil, grpcx.Err(codes.NotFound, "unknown sku")
	case err != nil:
		return nil, grpcx.Err(codes.Internal, err.Error())
	default:
		return &heliosv1.ReserveResponse{ReservationId: resID, Status: "pending"}, nil
	}
}

func (g *inventoryGRPC) Release(ctx context.Context, req *heliosv1.ReleaseRequest) (*heliosv1.ReleaseResponse, error) {
	ident := tenant.MustFrom(ctx)
	if req.OrderId == "" || req.Sku == "" {
		return nil, grpcx.Err(codes.InvalidArgument, "order_id and sku required")
	}
	err := database.InTx(ctx, g.svc.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var resID, sku, wh string
		var qty int64
		err := tx.QueryRow(ctx, `
			UPDATE reservations SET status='released'
			WHERE tenant_id=$1 AND order_id=$2 AND sku=$3 AND status='pending'
			RETURNING id, sku, warehouse_id, quantity`,
			ident.TenantID, req.OrderId, req.Sku).Scan(&resID, &sku, &wh, &qty)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `
			UPDATE stock SET reserved = reserved - $1, version = version + 1
			WHERE tenant_id=$2 AND warehouse_id=$3 AND sku=$4`, qty, ident.TenantID, wh, sku); err != nil {
			return err
		}
		env, err := events.New(ctx, events.InventoryReleased, "inventory", "reservation", resID, events.InventoryReservePayload{
			ReservationID: resID, OrderID: req.OrderId, SKU: sku, WarehouseID: wh, Quantity: qty,
		})
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, env)
	})
	if err != nil {
		return nil, grpcx.Err(codes.FailedPrecondition, "no pending reservation")
	}
	return &heliosv1.ReleaseResponse{Status: "released"}, nil
}
