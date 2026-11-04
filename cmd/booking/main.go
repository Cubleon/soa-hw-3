package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "flightbooking/gen/flight/v1"
	"flightbooking/internal/booking"
	"flightbooking/internal/config"
	"flightbooking/internal/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	attempt, err := config.Duration("GRPC_ATTEMPT_TIMEOUT", time.Second)
	if err != nil {
		return err
	}
	backoff, err := config.Duration("GRPC_RETRY_BACKOFF", 100*time.Millisecond)
	if err != nil {
		return err
	}
	threshold, err := config.Int("CB_FAILURE_THRESHOLD", 5)
	if err != nil {
		return err
	}
	window, err := config.Duration("CB_WINDOW", 30*time.Second)
	if err != nil {
		return err
	}
	openTimeout, err := config.Duration("CB_OPEN_TIMEOUT", 10*time.Second)
	if err != nil {
		return err
	}
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = db.Ping(ctx); err != nil {
		return err
	}
	breaker := rpc.NewBreaker(threshold, window, openTimeout, log)
	conn, err := grpc.NewClient(config.String("FLIGHT_GRPC_ADDR", "flight:50051"), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDisableRetry(), grpc.WithChainUnaryInterceptor(breaker.Interceptor(), rpc.Retry(attempt, backoff, log), rpc.AuthClient(key)))
	if err != nil {
		return err
	}
	defer conn.Close()
	svc := &booking.Service{DB: db, Flights: pb.NewFlightServiceClient(conn), Log: log}
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); svc.RunRecovery(ctx) }()
	defer func() { stop(); <-workerDone }()
	server := &http.Server{Addr: config.String("HTTP_ADDR", ":8080"), Handler: svc.Handler(), ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 25 * time.Second, IdleTimeout: 60 * time.Second}
	log.Info("booking HTTP listening", "address", server.Addr)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.ListenAndServe() }()
	select {
	case err = <-serveDone:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err = server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		return nil
	}
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("booking stopped", "error", err)
		os.Exit(1)
	}
}
