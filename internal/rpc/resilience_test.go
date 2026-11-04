package rpc

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func testLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestRetryOnlyTransientFailures(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.InvalidArgument, codes.NotFound, codes.ResourceExhausted, codes.Unauthenticated, codes.Internal, codes.OK} {
		t.Run(code.String(), func(t *testing.T) {
			calls := 0
			invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				calls++
				return status.Error(code, "test")
			}
			err := Retry(time.Second, time.Millisecond, testLog())(context.Background(), "test", nil, nil, nil, invoker)
			want := 1
			if code == codes.Unavailable || code == codes.DeadlineExceeded {
				want = 3
			}
			if calls != want || status.Code(err) != code {
				t.Fatalf("calls=%d code=%s", calls, status.Code(err))
			}
		})
	}
}

func TestRetryTimeoutAndCancellation(t *testing.T) {
	calls := 0
	invoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		calls++
		<-ctx.Done()
		return status.FromContextError(ctx.Err()).Err()
	}
	err := Retry(time.Millisecond, time.Millisecond, testLog())(context.Background(), "test", nil, nil, nil, invoker)
	if calls != 3 || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls = 0
	err = Retry(time.Second, time.Millisecond, testLog())(ctx, "test", nil, nil, nil, invoker)
	if calls != 0 || status.Code(err) != codes.Canceled {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestRetryEventuallySucceeds(t *testing.T) {
	calls := 0
	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		calls++
		if calls < 3 {
			return status.Error(codes.Unavailable, "temporary")
		}
		return nil
	}
	if err := Retry(time.Second, time.Millisecond, testLog())(context.Background(), "test", nil, nil, nil, invoker); err != nil || calls != 3 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestBreakerLifecycleAndSingleProbe(t *testing.T) {
	b := NewBreaker(2, time.Minute, 10*time.Second, testLog())
	now := time.Now()
	b.now = func() time.Time { return now }
	var calls atomic.Int32
	fail := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		calls.Add(1)
		return status.Error(codes.Unavailable, "down")
	}
	call := func(next grpc.UnaryInvoker) error {
		return b.Interceptor()(context.Background(), "test", nil, nil, nil, next)
	}
	_ = call(fail)
	_ = call(fail)
	if b.state != "OPEN" {
		t.Fatalf("state=%s", b.state)
	}
	_ = call(fail)
	if calls.Load() != 2 {
		t.Fatal("open breaker made a network call")
	}
	now = now.Add(11 * time.Second)
	started, finish := make(chan struct{}), make(chan struct{})
	probe := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		calls.Add(1)
		close(started)
		<-finish
		return nil
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = call(probe) }()
	<-started
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if status.Code(call(fail)) != codes.Unavailable {
				t.Error("concurrent half-open request was admitted")
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 3 {
		t.Fatalf("more than one probe: calls=%d", calls.Load())
	}
	close(finish)
	<-done
	if b.state != "CLOSED" {
		t.Fatalf("state=%s", b.state)
	}
	_ = call(fail)
	_ = call(fail)
	now = now.Add(11 * time.Second)
	_ = call(fail)
	if b.state != "OPEN" {
		t.Fatal("failed probe must reopen breaker")
	}
}

func TestBreakerWindowAndBusinessErrors(t *testing.T) {
	b := NewBreaker(2, time.Second, time.Second, testLog())
	now := time.Now()
	b.now = func() time.Time { return now }
	g, _ := b.before()
	b.after(g, status.Error(codes.Unavailable, "down"))
	now = now.Add(2 * time.Second)
	g, _ = b.before()
	b.after(g, status.Error(codes.Unavailable, "down"))
	if b.state != "CLOSED" {
		t.Fatal("expired failure window was counted")
	}
	g, _ = b.before()
	b.after(g, status.Error(codes.ResourceExhausted, "sold out"))
	g, _ = b.before()
	b.after(g, status.Error(codes.Unavailable, "down"))
	if b.state != "CLOSED" {
		t.Fatal("business errors should reset consecutive failures")
	}
}

func TestAuthRejectsAllMissingInvalidOrDuplicateCredentials(t *testing.T) {
	interceptor := AuthServer("secret")
	for _, values := range [][]string{nil, {"bad"}, {"secret", "secret"}} {
		md := metadata.MD{}
		for _, v := range values {
			md.Append("x-api-key", v)
		}
		_, err := interceptor(metadata.NewIncomingContext(context.Background(), md), nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) { t.Fatal("unauthorized call reached handler"); return nil, nil })
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("err=%v", err)
		}
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", "secret"))
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
}
