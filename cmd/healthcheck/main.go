package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"flightbooking/internal/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func check() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if len(os.Args) > 1 && os.Args[1] == "grpc" {
		conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(rpc.AuthClient(os.Getenv("GRPC_API_KEY"))))
		if err != nil {
			return err
		}
		defer conn.Close()
		resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if resp.Status != healthpb.HealthCheckResponse_SERVING {
			return fmt.Errorf("not serving")
		}
		return nil
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:8080/healthz", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}
func main() {
	if err := check(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
