package main

import (
	"context"

	heliosv1 "github.com/helios-commerce/helios/gen/helios/v1"
	"github.com/helios-commerce/helios/platform/database"
	"github.com/helios-commerce/helios/platform/events"
	"github.com/helios-commerce/helios/platform/grpcx"
	"github.com/helios-commerce/helios/platform/id"
	"github.com/helios-commerce/helios/platform/outbox"
	"github.com/helios-commerce/helios/platform/tenant"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
)

type fulfillmentGRPC struct {
	heliosv1.UnimplementedFulfillmentServiceServer
	svc *fulfillment
}

func (g *fulfillmentGRPC) Allocate(ctx context.Context, req *heliosv1.AllocateRequest) (*heliosv1.AllocateResponse, error) {
	ident := tenant.MustFrom(ctx)
	if req.OrderId == "" {
		return nil, grpcx.Err(codes.InvalidArgument, "order_id required")
	}
	wh := req.WarehouseId
	if wh == "" {
		wh = "wh-default"
	}
	sid := id.NewULID()
	err := database.InTx(ctx, g.svc.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO shipments (id, tenant_id, order_id, warehouse_id, status)
			VALUES ($1,$2,$3,$4,'allocated')
			ON CONFLICT (tenant_id, order_id) DO NOTHING`, sid, ident.TenantID, req.OrderId, wh); err != nil {
			return err
		}
		env, err := events.New(ctx, events.FulfillmentAllocated, "fulfillment", "shipment", sid, events.FulfillmentPayload{
			ShipmentID: sid, OrderID: req.OrderId, WarehouseID: wh,
		})
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, env)
	})
	if err != nil {
		return nil, grpcx.Err(codes.Internal, err.Error())
	}
	return &heliosv1.AllocateResponse{ShipmentId: sid, Status: "allocated"}, nil
}
