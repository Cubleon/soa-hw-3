package rpc

import (
	"context"
	"crypto/subtle"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func authenticate(ctx context.Context, key string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("x-api-key")
	if key == "" || len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(key)) != 1 {
		return status.Error(codes.Unauthenticated, "valid service API key is required")
	}
	return nil
}

func AuthServer(key string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if err := authenticate(ctx, key); err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

func AuthStreamServer(key string) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		if err := authenticate(stream.Context(), key); err != nil {
			return err
		}
		return next(srv, stream)
	}
}

func AuthClient(key string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, next grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		md, _ := metadata.FromOutgoingContext(ctx)
		md = md.Copy()
		md.Set("x-api-key", key)
		return next(metadata.NewOutgoingContext(ctx, md), method, req, reply, cc, opts...)
	}
}
