package rpc

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func transient(err error) bool {
	c := status.Code(err)
	return c == codes.Unavailable || c == codes.DeadlineExceeded
}

// Retry wraps individual network attempts; three attempts means two sleeps.
func Retry(attemptTimeout, backoff time.Duration, log *slog.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, next grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			if ctx.Err() != nil {
				return status.FromContextError(ctx.Err()).Err()
			}
			if attempt > 0 {
				delay := backoff * time.Duration(1<<(attempt-1))
				log.Warn("grpc retry", "method", method, "attempt", attempt+1, "backoff", delay)
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return status.FromContextError(ctx.Err()).Err()
				case <-timer.C:
				}
			}
			callCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
			err = next(callCtx, method, req, reply, cc, opts...)
			cancel()
			if !transient(err) {
				return err
			}
		}
		return err
	}
}

type Breaker struct {
	mu                    sync.Mutex
	state                 string
	failures              int
	windowStart, openedAt time.Time
	threshold             int
	window, openTimeout   time.Duration
	generation            uint64
	log                   *slog.Logger
	now                   func() time.Time
}

func NewBreaker(threshold int, window, openTimeout time.Duration, log *slog.Logger) *Breaker {
	return &Breaker{state: "CLOSED", threshold: threshold, window: window, openTimeout: openTimeout, log: log, now: time.Now}
}

func (b *Breaker) transition(state string, now time.Time) {
	b.log.Warn("circuit breaker transition", "from", b.state, "to", state)
	b.state = state
	b.generation++
	b.failures = 0
	b.windowStart = now
	if state == "OPEN" {
		b.openedAt = now
	}
}

func (b *Breaker) before() (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	if b.state == "OPEN" && now.Sub(b.openedAt) >= b.openTimeout {
		b.transition("HALF_OPEN", now)
		return b.generation, nil
	}
	if b.state != "CLOSED" {
		return 0, status.Error(codes.Unavailable, "flight service circuit is open; retry later")
	}
	return b.generation, nil
}

func (b *Breaker) after(generation uint64, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if generation != b.generation {
		return
	} // Ignore calls from an older state.
	now := b.now()
	if b.state == "HALF_OPEN" {
		if transient(err) {
			b.transition("OPEN", now)
		} else {
			b.transition("CLOSED", now)
		}
		return
	}
	if !transient(err) {
		b.failures = 0
		b.windowStart = now
		return
	}
	if b.windowStart.IsZero() || now.Sub(b.windowStart) > b.window {
		b.failures = 0
		b.windowStart = now
	}
	b.failures++
	if b.failures >= b.threshold {
		b.transition("OPEN", now)
	}
}

// Place outside Retry: one logical failed call counts once, not three times.
func (b *Breaker) Interceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, next grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		generation, err := b.before()
		if err != nil {
			return err
		}
		err = next(ctx, method, req, reply, cc, opts...)
		b.after(generation, err)
		return err
	}
}
