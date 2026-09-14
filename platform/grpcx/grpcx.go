package grpcx

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"

	"github.com/helios-commerce/helios/platform/tenant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	MDInternal = "x-helios-internal"
	MDTenant   = "x-tenant-id"
	MDUser     = "x-user-id"
)

// Server listens on addr (e.g. ":9090") and applies the internal-token interceptor.
func NewServer(internalToken string, opts ...grpc.ServerOption) *grpc.Server {
	unary := grpc.UnaryInterceptor(InternalAuthUnary(internalToken))
	all := append([]grpc.ServerOption{unary}, opts...)
	return grpc.NewServer(all...)
}

func ListenAndServe(log *slog.Logger, addr string, srv *grpc.Server) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Info("grpc listen", "addr", addr)
	return srv.Serve(ln)
}

func Dial(ctx context.Context, target string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
}

// Outgoing attaches mesh auth metadata for orchestrator → workers.
func Outgoing(ctx context.Context, internalToken string, ident tenant.Identity) context.Context {
	md := metadata.Pairs(
		MDInternal, internalToken,
		MDTenant, ident.TenantID,
		MDUser, ident.UserID,
	)
	return metadata.NewOutgoingContext(ctx, md)
}

func InternalAuthUnary(internalToken string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}
		if first(md.Get(MDInternal)) != internalToken {
			return nil, status.Error(codes.Unauthenticated, "invalid internal token")
		}
		tid := first(md.Get(MDTenant))
		if tid == "" {
			return nil, status.Error(codes.InvalidArgument, "x-tenant-id required")
		}
		uid := first(md.Get(MDUser))
		ctx = tenant.With(ctx, tenant.Identity{TenantID: tid, UserID: uid, Role: "service"})
		return handler(ctx, req)
	}
}

func first(v []string) string {
	if len(v) == 0 {
		return ""
	}
	return strings.TrimSpace(v[0])
}

func Err(code codes.Code, msg string) error {
	return status.Error(code, msg)
}

func Errf(code codes.Code, format string, args ...any) error {
	return status.Errorf(code, format, args...)
}

func MustAddr(envVal, def string) string {
	if strings.TrimSpace(envVal) != "" {
		return envVal
	}
	return def
}

func ParseHostPort(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return fmt.Sprintf("0.0.0.0%s", addr)
	}
	return addr
}
