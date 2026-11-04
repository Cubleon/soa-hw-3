package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pb "flightbooking/gen/flight/v1"
	"flightbooking/internal/config"
	"flightbooking/internal/flight"
	"flightbooking/internal/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	dsn, err := config.Required("DATABASE_URL")
	if err != nil {
		return err
	}
	key, err := config.Required("GRPC_API_KEY")
	if err != nil {
		return err
	}
	ttl, err := config.Duration("CACHE_TTL", 5*time.Minute)
	if err != nil {
		return err
	}
	if ttl < 5*time.Minute || ttl > 10*time.Minute {
		return fmt.Errorf("CACHE_TTL must be between 5m and 10m")
	}
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = db.Ping(ctx); err != nil {
		return err
	}
	client := redis.NewFailoverClient(&redis.FailoverOptions{MasterName: config.String("REDIS_MASTER_NAME", "flightmaster"),
		SentinelAddrs: strings.Split(config.String("REDIS_SENTINELS", "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379"), ","),
		Password:      os.Getenv("REDIS_PASSWORD"), SentinelPassword: os.Getenv("REDIS_SENTINEL_PASSWORD"),
		DialTimeout: 200 * time.Millisecond, ReadTimeout: 200 * time.Millisecond, WriteTimeout: 200 * time.Millisecond, MaxRetries: -1, ContextTimeoutEnabled: true})
	defer client.Close()
	if err = client.Ping(ctx).Err(); err != nil {
		log.Warn("Redis unavailable at startup; cache bypass enabled", "error", err)
	}
	listener, err := net.Listen("tcp", config.String("GRPC_ADDR", ":50051"))
	if err != nil {
		return err
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(rpc.AuthServer(key)), grpc.StreamInterceptor(rpc.AuthStreamServer(key)))
	pb.RegisterFlightServiceServer(server, flight.NewServer(db, flight.NewCache(client, ttl, log), log))
	h := health.NewServer()
	healthpb.RegisterHealthServer(server, h)
	h.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	log.Info("flight gRPC listening", "address", listener.Addr().String())
	select {
	case err = <-serveDone:
		return err
	case <-ctx.Done():
		h.Shutdown()
		done := make(chan struct{})
		go func() { server.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			server.Stop()
		}
		return nil
	}
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("flight stopped", "error", err)
		os.Exit(1)
	}
}
