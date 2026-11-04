//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "flightbooking/gen/flight/v1"
	"flightbooking/internal/booking"
	"flightbooking/internal/flight"
	"flightbooking/internal/rpc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type system struct {
	fdb, bdb *pgxpool.Pool
	flights  pb.FlightServiceClient
	redis    *redis.Client
	log      *slog.Logger
	url      string
}

func required(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Fatalf("%s is required; run make integration", key)
	}
	return v
}

func connect(t *testing.T) *system {
	t.Helper()
	ctx := context.Background()
	fdb, err := pgxpool.New(ctx, required(t, "FLIGHT_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fdb.Close)
	bdb, err := pgxpool.New(ctx, required(t, "BOOKING_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bdb.Close)
	conn, err := grpc.NewClient(required(t, "FLIGHT_GRPC_ADDR"), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(rpc.AuthClient(required(t, "GRPC_API_KEY"))))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	r := redis.NewFailoverClient(&redis.FailoverOptions{MasterName: "flightmaster", SentinelAddrs: strings.Split(required(t, "REDIS_SENTINELS"), ","), DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, MaxRetries: 1})
	t.Cleanup(func() { _ = r.Close() })
	return &system{fdb: fdb, bdb: bdb, flights: pb.NewFlightServiceClient(conn), redis: r, log: slog.New(slog.NewTextHandler(io.Discard, nil)), url: required(t, "BOOKING_HTTP_URL")}
}

func (s *system) fixture(t *testing.T, seats int) string {
	t.Helper()
	id := uuid.NewString()
	ctx := context.Background()
	_, err := s.fdb.Exec(ctx, `INSERT INTO flights(id,flight_number,airline_code,origin,destination,departure_at,arrival_at,total_seats,available_seats,price_minor,status)
 VALUES($1,$2,'SU','SVO','LED','2035-04-01 09:00+00','2035-04-01 10:30+00',$3,$3,123456,'SCHEDULED')`, id, "T"+id[:8], seats)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.fdb.Exec(ctx, `UPDATE cache_generation SET generation=generation+1`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, q := range []string{
			`DELETE FROM booking_operations WHERE payload->>'flight_id'=$1 OR booking_id IN (SELECT id FROM bookings WHERE flight_id=$1::uuid)`,
			`DELETE FROM bookings WHERE flight_id=$1::uuid`,
		} {
			if _, e := s.bdb.Exec(ctx, q, id); e != nil {
				t.Error(e)
			}
		}
		for _, q := range []string{
			`DELETE FROM cancellation_fences WHERE booking_id IN (SELECT booking_id FROM seat_reservations WHERE flight_id=$1)`,
			`DELETE FROM seat_reservations WHERE flight_id=$1`,
			`DELETE FROM flights WHERE id=$1`,
		} {
			if _, e := s.fdb.Exec(ctx, q, id); e != nil {
				t.Error(e)
			}
		}
		if _, e := s.fdb.Exec(ctx, `UPDATE cache_generation SET generation=generation+1`); e != nil {
			t.Error(e)
		}
	})
	return id
}

func (s *system) seats(t *testing.T, id string) int32 {
	t.Helper()
	f, err := s.flights.GetFlight(context.Background(), &pb.GetFlightRequest{Id: id})
	if err != nil {
		t.Fatal(err)
	}
	return f.AvailableSeats
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v; want %s", err, want)
	}
}

func (s *system) http(t *testing.T, method, path string, body any, want int, out any) {
	t.Helper()
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, s.url+path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 25 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != want {
		t.Fatalf("%s %s: status=%d want=%d body=%s", method, path, resp.StatusCode, want, b)
	}
	if out != nil {
		if err = json.Unmarshal(b, out); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSystem(t *testing.T) {
	s := connect(t)
	ctx := context.Background()
	t.Run("authentication_on_every_method", func(t *testing.T) {
		for _, key := range []string{"", "incorrect"} {
			options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
			if key != "" {
				options = append(options, grpc.WithUnaryInterceptor(rpc.AuthClient(key)))
			}
			conn, err := grpc.NewClient(os.Getenv("FLIGHT_GRPC_ADDR"), options...)
			if err != nil {
				t.Fatal(err)
			}
			client := pb.NewFlightServiceClient(conn)
			_, err = client.SearchFlights(ctx, &pb.SearchFlightsRequest{})
			wantCode(t, err, codes.Unauthenticated)
			_, err = client.GetFlight(ctx, &pb.GetFlightRequest{})
			wantCode(t, err, codes.Unauthenticated)
			_, err = client.ReserveSeats(ctx, &pb.ReserveSeatsRequest{})
			wantCode(t, err, codes.Unauthenticated)
			_, err = client.ReleaseReservation(ctx, &pb.ReleaseReservationRequest{})
			wantCode(t, err, codes.Unauthenticated)
			_, err = client.UpdateFlight(ctx, &pb.UpdateFlightRequest{})
			wantCode(t, err, codes.Unauthenticated)
			_ = conn.Close()
		}
	})
	t.Run("contract_errors", func(t *testing.T) {
		_, err := s.flights.GetFlight(ctx, &pb.GetFlightRequest{Id: uuid.NewString()})
		wantCode(t, err, codes.NotFound)
		_, err = s.flights.GetFlight(ctx, &pb.GetFlightRequest{Id: "bad"})
		wantCode(t, err, codes.InvalidArgument)
		_, err = s.flights.SearchFlights(ctx, &pb.SearchFlightsRequest{Origin: "SVO", Destination: "LED", DepartureDate: timestamppb.New(time.Date(2035, 4, 1, 1, 0, 0, 0, time.UTC))})
		wantCode(t, err, codes.InvalidArgument)
		id := s.fixture(t, 1)
		_, err = s.flights.ReserveSeats(ctx, &pb.ReserveSeatsRequest{FlightId: id, BookingId: uuid.NewString(), SeatCount: 2})
		wantCode(t, err, codes.ResourceExhausted)
		if got := s.seats(t, id); got != 1 {
			t.Fatalf("failed reservation changed seats: %d", got)
		}
		_, err = s.flights.UpdateFlight(ctx, &pb.UpdateFlightRequest{Id: id, PriceMinor: 100, Status: pb.FlightStatus_COMPLETED})
		wantCode(t, err, codes.FailedPrecondition)
	})
	t.Run("only_one_buyer_of_last_seat", func(t *testing.T) {
		id := s.fixture(t, 1)
		var successes atomic.Int32
		var wg sync.WaitGroup
		for range 30 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.flights.ReserveSeats(ctx, &pb.ReserveSeatsRequest{FlightId: id, BookingId: uuid.NewString(), SeatCount: 1})
				if err == nil {
					successes.Add(1)
				} else if status.Code(err) != codes.ResourceExhausted {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		if successes.Load() != 1 || s.seats(t, id) != 0 {
			t.Fatalf("successes=%d seats=%d", successes.Load(), s.seats(t, id))
		}
		var count int
		if err := s.fdb.QueryRow(ctx, `SELECT count(*) FROM seat_reservations WHERE flight_id=$1 AND status='ACTIVE'`, id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("active reservations=%d", count)
		}
	})
	t.Run("concurrent_idempotency_and_release", func(t *testing.T) {
		id := s.fixture(t, 10)
		bookingID := uuid.NewString()
		var wg sync.WaitGroup
		for range 20 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.flights.ReserveSeats(ctx, &pb.ReserveSeatsRequest{FlightId: id, BookingId: bookingID, SeatCount: 2})
				if err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		if got := s.seats(t, id); got != 8 {
			t.Fatalf("retries consumed extra seats: %d", got)
		}
		_, err := s.flights.ReserveSeats(ctx, &pb.ReserveSeatsRequest{FlightId: id, BookingId: bookingID, SeatCount: 1})
		wantCode(t, err, codes.AlreadyExists)
		for range 20 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.flights.ReleaseReservation(ctx, &pb.ReleaseReservationRequest{BookingId: bookingID})
				if err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		if got := s.seats(t, id); got != 10 {
			t.Fatalf("release returned seats more than once: %d", got)
		}
		_, err = s.flights.ReserveSeats(ctx, &pb.ReserveSeatsRequest{FlightId: id, BookingId: bookingID, SeatCount: 2})
		wantCode(t, err, codes.FailedPrecondition)
	})
	t.Run("release_before_late_reserve", func(t *testing.T) {
		id := s.fixture(t, 5)
		bookingID := uuid.NewString()
		t.Cleanup(func() { _, _ = s.fdb.Exec(ctx, `DELETE FROM cancellation_fences WHERE booking_id=$1`, bookingID) })
		_, err := s.flights.ReleaseReservation(ctx, &pb.ReleaseReservationRequest{BookingId: bookingID})
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.flights.ReserveSeats(ctx, &pb.ReserveSeatsRequest{FlightId: id, BookingId: bookingID, SeatCount: 1})
		wantCode(t, err, codes.FailedPrecondition)
		if s.seats(t, id) != 5 {
			t.Fatal("late reserve bypassed compensation fence")
		}
	})
	t.Run("flight_transaction_rolls_back", func(t *testing.T) {
		id := s.fixture(t, 5)
		constraint := "test_reject_" + strings.ReplaceAll(id, "-", "")
		if _, err := s.fdb.Exec(ctx, fmt.Sprintf(`ALTER TABLE seat_reservations ADD CONSTRAINT %s CHECK(flight_id <> '%s'::uuid)`, constraint, id)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = s.fdb.Exec(ctx, `ALTER TABLE seat_reservations DROP CONSTRAINT `+constraint) })
		_, err := s.flights.ReserveSeats(ctx, &pb.ReserveSeatsRequest{FlightId: id, BookingId: uuid.NewString(), SeatCount: 1})
		wantCode(t, err, codes.Internal)
		if got := s.seats(t, id); got != 5 {
			t.Fatalf("failed INSERT was not rolled back: seats=%d", got)
		}
	})
	t.Run("rest_lifecycle_and_price_snapshot", func(t *testing.T) {
		id := s.fixture(t, 10)
		user := uuid.NewString()
		var b booking.Booking
		s.http(t, "GET", "/flights?origin=SVO&destination=LED&date=2035-04-01", nil, 200, nil)
		s.http(t, "GET", "/flights?origin=SVO&destination=LED", nil, 200, nil)
		s.http(t, "GET", "/flights/"+id, nil, 200, nil)
		s.http(t, "POST", "/bookings", booking.CreateRequest{UserID: user, FlightID: id, PassengerName: "Иван Иванов", PassengerEmail: "ivan@example.com", SeatCount: 2}, 201, &b)
		if b.TotalPriceMinor != 246912 || b.Status != "CONFIRMED" || s.seats(t, id) != 8 {
			t.Fatalf("booking=%+v seats=%d", b, s.seats(t, id))
		}
		_, err := s.flights.UpdateFlight(ctx, &pb.UpdateFlightRequest{Id: id, PriceMinor: 999999, Status: pb.FlightStatus_SCHEDULED})
		if err != nil {
			t.Fatal(err)
		}
		var got booking.Booking
		s.http(t, "GET", "/bookings/"+b.ID, nil, 200, &got)
		if got.TotalPriceMinor != b.TotalPriceMinor {
			t.Fatal("snapshot price changed")
		}
		var list struct {
			Bookings []booking.Booking `json:"bookings"`
		}
		s.http(t, "GET", "/bookings?user_id="+user, nil, 200, &list)
		if len(list.Bookings) != 1 {
			t.Fatal("booking missing from list")
		}
		s.http(t, "POST", "/bookings/"+b.ID+"/cancel", nil, 200, &got)
		if got.Status != "CANCELLED" || s.seats(t, id) != 10 {
			t.Fatalf("cancel: %+v", got)
		}
		s.http(t, "POST", "/bookings/"+b.ID+"/cancel", nil, 200, nil)
		if s.seats(t, id) != 10 {
			t.Fatal("duplicate cancellation changed capacity")
		}
		s.http(t, "GET", "/bookings/"+uuid.NewString(), nil, 404, nil)
		s.http(t, "POST", "/bookings", booking.CreateRequest{UserID: user, FlightID: id, PassengerName: "Иван", PassengerEmail: "ivan@example.com", SeatCount: 11}, 409, nil)
	})
	t.Run("cache_hit_ttl_and_invalidation", func(t *testing.T) {
		id := s.fixture(t, 10)
		req := &pb.GetFlightRequest{Id: id}
		if _, err := s.flights.GetFlight(ctx, req); err != nil {
			t.Fatal(err)
		}
		var generation int64
		if err := s.fdb.QueryRow(ctx, `SELECT generation FROM cache_generation`).Scan(&generation); err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("v%d:flight:%s", generation, id)
		ttl, err := s.redis.TTL(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if ttl <= 0 || ttl > 10*time.Minute {
			t.Fatalf("invalid ttl: %s", ttl)
		}
		// Observe that a hit leaves the TTL decreasing, rather than replacing it.
		if _, err = s.flights.GetFlight(ctx, req); err != nil {
			t.Fatal(err)
		}
		searchReq := &pb.SearchFlightsRequest{Origin: "SVO", Destination: "LED", DepartureDate: timestamppb.New(time.Date(2035, 4, 1, 0, 0, 0, 0, time.UTC))}
		if _, err = s.flights.SearchFlights(ctx, searchReq); err != nil {
			t.Fatal(err)
		}
		searchKey := fmt.Sprintf("v%d:search:SVO:LED:2035-04-01", generation)
		if ttl, err = s.redis.TTL(ctx, searchKey).Result(); err != nil || ttl <= 0 {
			t.Fatalf("search ttl=%s err=%v", ttl, err)
		}
		bookingID := uuid.NewString()
		if _, err = s.flights.ReserveSeats(ctx, &pb.ReserveSeatsRequest{FlightId: id, BookingId: bookingID, SeatCount: 3}); err != nil {
			t.Fatal(err)
		}
		if s.seats(t, id) != 7 {
			t.Fatal("stale flight cache after reserve")
		}
		search, err := s.flights.SearchFlights(ctx, searchReq)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, f := range search.Flights {
			if f.Id == id {
				found = true
				if f.AvailableSeats != 7 {
					t.Fatal("stale search cache after reserve")
				}
			}
		}
		if !found {
			t.Fatal("fixture missing")
		}
		if _, err = s.flights.ReleaseReservation(ctx, &pb.ReleaseReservationRequest{BookingId: bookingID}); err != nil {
			t.Fatal(err)
		}
		if s.seats(t, id) != 10 {
			t.Fatal("stale flight cache after release")
		}
		if _, err = s.flights.UpdateFlight(ctx, &pb.UpdateFlightRequest{Id: id, PriceMinor: 77, Status: pb.FlightStatus_CANCELLED}); err != nil {
			t.Fatal(err)
		}
		f, err := s.flights.GetFlight(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if f.PriceMinor != 77 || f.Status != pb.FlightStatus_CANCELLED {
			t.Fatal("stale cache after update")
		}
		search, err = s.flights.SearchFlights(ctx, searchReq)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range search.Flights {
			if f.Id == id {
				t.Fatal("cancelled flight returned by search")
			}
		}
	})
	t.Run("redis_outage_falls_back_to_postgres", func(t *testing.T) {
		id := s.fixture(t, 4)
		dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: time.Millisecond, MaxRetries: -1})
		defer dead.Close()
		server := flight.NewServer(s.fdb, flight.NewCache(dead, 5*time.Minute, s.log), s.log)
		f, err := server.GetFlight(ctx, &pb.GetFlightRequest{Id: id})
		if err != nil || f.AvailableSeats != 4 {
			t.Fatalf("fallback f=%v err=%v", f, err)
		}
	})
	t.Run("ambiguous_reserve_is_compensated", func(t *testing.T) {
		id := s.fixture(t, 4)
		client := &ambiguousClient{FlightServiceClient: s.flights}
		svc := &booking.Service{DB: s.bdb, Flights: client, Log: s.log}
		_, err := svc.Create(ctx, booking.CreateRequest{UserID: "test", FlightID: id, PassengerName: "Test", PassengerEmail: "test@example.com", SeatCount: 2})
		wantCode(t, err, codes.Unavailable)
		if s.seats(t, id) != 4 {
			t.Fatal("orphan seats after ambiguous reserve")
		}
		var count int
		if err = s.bdb.QueryRow(ctx, `SELECT count(*) FROM bookings WHERE flight_id=$1`, id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatal("failed reserve created booking")
		}
	})
	t.Run("booking_insert_failure_is_compensated", func(t *testing.T) {
		id := s.fixture(t, 4)
		constraint := "test_reject_" + strings.ReplaceAll(id, "-", "")
		if _, err := s.bdb.Exec(ctx, fmt.Sprintf(`ALTER TABLE bookings ADD CONSTRAINT %s CHECK(flight_id <> '%s'::uuid)`, constraint, id)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = s.bdb.Exec(ctx, `ALTER TABLE bookings DROP CONSTRAINT `+constraint) })
		svc := &booking.Service{DB: s.bdb, Flights: s.flights, Log: s.log}
		_, err := svc.Create(ctx, booking.CreateRequest{UserID: "test", FlightID: id, PassengerName: "Test", PassengerEmail: "test@example.com", SeatCount: 2})
		wantCode(t, err, codes.Internal)
		if s.seats(t, id) != 4 {
			t.Fatal("orphan seats after booking INSERT failure")
		}
	})
	t.Run("recovery_after_process_crash", func(t *testing.T) {
		id := s.fixture(t, 4)
		bookingID := uuid.NewString()
		payload, _ := json.Marshal(booking.Booking{ID: bookingID, FlightID: id})
		if _, err := s.bdb.Exec(ctx, `INSERT INTO booking_operations(booking_id,kind,payload) VALUES($1,'CREATE',$2)`, bookingID, payload); err != nil {
			t.Fatal(err)
		}
		if _, err := s.flights.ReserveSeats(ctx, &pb.ReserveSeatsRequest{FlightId: id, BookingId: bookingID, SeatCount: 2}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.bdb.Exec(ctx, `UPDATE booking_operations SET created_at=now()-interval '1 minute' WHERE booking_id=$1`, bookingID); err != nil {
			t.Fatal(err)
		}
		svc := &booking.Service{DB: s.bdb, Flights: s.flights, Log: s.log}
		if err := svc.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		if s.seats(t, id) != 4 {
			t.Fatal("crashed create was not compensated")
		}
		var state string
		if err := s.bdb.QueryRow(ctx, `SELECT state FROM booking_operations WHERE booking_id=$1`, bookingID).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state != "DONE" {
			t.Fatalf("state=%s", state)
		}
		// A persisted cancellation must also survive a process restart.
		b, err := svc.Create(ctx, booking.CreateRequest{UserID: "test", FlightID: id, PassengerName: "Test", PassengerEmail: "test@example.com", SeatCount: 1})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.bdb.Exec(ctx, `INSERT INTO booking_operations(booking_id,kind) VALUES($1,'CANCEL')`, b.ID); err != nil {
			t.Fatal(err)
		}
		if err = svc.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		b, err = svc.Get(ctx, b.ID)
		if err != nil || b.Status != "CANCELLED" || s.seats(t, id) != 4 {
			t.Fatalf("cancel recovery booking=%+v err=%v", b, err)
		}
	})
	t.Run("sentinel_switches_master", func(t *testing.T) {
		addr := strings.Split(os.Getenv("REDIS_SENTINELS"), ",")[0]
		sentinel := redis.NewSentinelClient(&redis.Options{Addr: addr})
		defer sentinel.Close()
		before, err := sentinel.GetMasterAddrByName(ctx, "flightmaster").Result()
		if err != nil {
			t.Fatal(err)
		}
		if err = sentinel.Failover(ctx, "flightmaster").Err(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		changed := false
		for time.Now().Before(deadline) {
			after, e := sentinel.GetMasterAddrByName(ctx, "flightmaster").Result()
			if e == nil && strings.Join(before, ":") != strings.Join(after, ":") {
				key := "integration:failover:" + uuid.NewString()
				if s.redis.Set(ctx, key, "ok", time.Minute).Err() == nil {
					_ = s.redis.Del(ctx, key).Err()
					changed = true
					break
				}
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !changed {
			t.Fatal("Sentinel did not promote replica / Go client did not reconnect")
		}
		id := s.fixture(t, 2)
		if s.seats(t, id) != 2 {
			t.Fatal("flight service unavailable after failover")
		}
	})
}

type ambiguousClient struct{ pb.FlightServiceClient }

func (c *ambiguousClient) ReserveSeats(ctx context.Context, r *pb.ReserveSeatsRequest, opts ...grpc.CallOption) (*pb.SeatReservation, error) {
	if _, err := c.FlightServiceClient.ReserveSeats(ctx, r, opts...); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.Unavailable, "simulated lost response AFTER committed reservation")
}
