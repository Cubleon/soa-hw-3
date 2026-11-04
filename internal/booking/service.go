package booking

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"time"

	pb "flightbooking/gen/flight/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CreateRequest struct {
	UserID         string `json:"user_id"`
	FlightID       string `json:"flight_id"`
	PassengerName  string `json:"passenger_name"`
	PassengerEmail string `json:"passenger_email"`
	SeatCount      int32  `json:"seat_count"`
}

type Booking struct {
	ID              string    `json:"id"`
	UserID          string    `json:"user_id"`
	FlightID        string    `json:"flight_id"`
	PassengerName   string    `json:"passenger_name"`
	PassengerEmail  string    `json:"passenger_email"`
	SeatCount       int32     `json:"seat_count"`
	TotalPriceMinor int64     `json:"total_price_minor"`
	Currency        string    `json:"currency"`
	Status          string    `json:"status"`
	CreatedAt       time.Time `json:"created_at"`
}

type Service struct {
	DB      *pgxpool.Pool
	Flights pb.FlightServiceClient
	Log     *slog.Logger
}

func (s *Service) dbError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return status.Error(codes.NotFound, "booking not found")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	s.Log.Error("booking database error", "error", err)
	return status.Error(codes.Internal, "booking storage error")
}

const bookingColumns = `id::text,user_id,flight_id::text,passenger_name,passenger_email,seat_count,total_price_minor,currency,status,created_at`

func scanBooking(row pgx.Row) (*Booking, error) {
	b := &Booking{}
	err := row.Scan(&b.ID, &b.UserID, &b.FlightID, &b.PassengerName, &b.PassengerEmail, &b.SeatCount, &b.TotalPriceMinor, &b.Currency, &b.Status, &b.CreatedAt)
	return b, err
}

func (s *Service) Get(ctx context.Context, id string) (*Booking, error) {
	b, err := scanBooking(s.DB.QueryRow(ctx, `SELECT `+bookingColumns+` FROM bookings WHERE id=$1`, id))
	if err != nil {
		return nil, s.dbError(err)
	}
	return b, nil
}

func (s *Service) List(ctx context.Context, user string) ([]*Booking, error) {
	rows, err := s.DB.Query(ctx, `SELECT `+bookingColumns+` FROM bookings WHERE user_id=$1 ORDER BY created_at DESC,id`, user)
	if err != nil {
		return nil, s.dbError(err)
	}
	defer rows.Close()
	out := []*Booking{}
	for rows.Next() {
		b, e := scanBooking(rows)
		if e != nil {
			return nil, s.dbError(e)
		}
		out = append(out, b)
	}
	if err = rows.Err(); err != nil {
		return nil, s.dbError(err)
	}
	return out, nil
}

func (s *Service) Create(ctx context.Context, r CreateRequest) (*Booking, error) {
	if err := validateCreate(r); err != nil {
		return nil, err
	}
	f, err := s.Flights.GetFlight(ctx, &pb.GetFlightRequest{Id: r.FlightID})
	if err != nil {
		return nil, err
	}
	if f.PriceMinor <= 0 || f.PriceMinor > math.MaxInt64/int64(r.SeatCount) {
		return nil, status.Error(codes.InvalidArgument, "total price is out of range")
	}
	b := &Booking{ID: uuid.NewString(), UserID: r.UserID, FlightID: r.FlightID, PassengerName: r.PassengerName,
		PassengerEmail: r.PassengerEmail, SeatCount: r.SeatCount, TotalPriceMinor: int64(r.SeatCount) * f.PriceMinor,
		Currency: f.Currency, Status: "CONFIRMED", CreatedAt: time.Now().UTC()}
	payload, _ := json.Marshal(b)
	// Persist the intent BEFORE contacting the other database. An interrupted
	// CREATE is compensated by the worker, never silently made into a booking.
	_, err = s.DB.Exec(ctx, `INSERT INTO booking_operations(booking_id,kind,payload) VALUES($1,'CREATE',$2)`, b.ID, payload)
	if err != nil {
		return nil, s.dbError(err)
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, s.dbError(err)
	}
	defer tx.Rollback(context.Background())
	var state string
	if err = tx.QueryRow(ctx, `SELECT state FROM booking_operations WHERE booking_id=$1 AND kind='CREATE' FOR UPDATE`, b.ID).Scan(&state); err != nil {
		return nil, s.dbError(err)
	}
	if state != "PENDING" {
		return nil, status.Error(codes.Aborted, "booking operation was already recovered")
	}
	_, err = s.Flights.ReserveSeats(ctx, &pb.ReserveSeatsRequest{FlightId: b.FlightID, BookingId: b.ID, SeatCount: b.SeatCount})
	if err != nil {
		if _, e := tx.Exec(ctx, `UPDATE booking_operations SET state='COMPENSATING' WHERE booking_id=$1 AND kind='CREATE'`, b.ID); e == nil {
			_ = tx.Commit(ctx)
		}
		// Release the row lock before synchronous best-effort compensation.
		_ = tx.Rollback(context.Background())
		s.compensateNow(b.ID)
		return nil, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO bookings(id,user_id,flight_id,passenger_name,passenger_email,seat_count,total_price_minor,currency,status,created_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, b.ID, b.UserID, b.FlightID, b.PassengerName, b.PassengerEmail, b.SeatCount, b.TotalPriceMinor, b.Currency, b.Status, b.CreatedAt)
	if err == nil {
		_, err = tx.Exec(ctx, `UPDATE booking_operations SET state='DONE' WHERE booking_id=$1 AND kind='CREATE'`, b.ID)
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		_ = tx.Rollback(context.Background())
		s.compensateNow(b.ID)
		return nil, s.dbError(err)
	}
	return b, nil
}

func (s *Service) compensateNow(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.recoverOne(ctx, id, "CREATE"); err != nil {
		s.Log.Warn("compensation queued", "booking_id", id, "error", err)
	}
}

func (s *Service) Cancel(ctx context.Context, id string) (*Booking, error) {
	b, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if b.Status == "CANCELLED" {
		return b, nil
	} // Safe repeated HTTP cancellation.
	_, err = s.DB.Exec(ctx, `INSERT INTO booking_operations(booking_id,kind) VALUES($1,'CANCEL') ON CONFLICT DO NOTHING`, id)
	if err != nil {
		return nil, s.dbError(err)
	}
	if err = s.recoverOne(ctx, id, "CANCEL"); err != nil {
		return nil, err
	}
	return s.Get(ctx, id)
}

// A durable operation row is the inter-process mutex. Recovery and handlers
// cannot apply the same operation concurrently, even with several replicas.
func (s *Service) recoverOne(ctx context.Context, id, kind string) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return s.dbError(err)
	}
	defer tx.Rollback(context.Background())
	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM booking_operations WHERE booking_id=$1 AND kind=$2 FOR UPDATE`, id, kind).Scan(&state)
	if err != nil {
		return s.dbError(err)
	}
	if state == "DONE" {
		return nil
	}
	if kind == "CREATE" {
		var exists bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM bookings WHERE id=$1)`, id).Scan(&exists); err != nil {
			return s.dbError(err)
		}
		// A commit can succeed even if its acknowledgement is lost. Never release
		// the seats of a booking that actually exists.
		if !exists {
			if _, err = s.Flights.ReleaseReservation(ctx, &pb.ReleaseReservationRequest{BookingId: id}); err != nil {
				return err
			}
			s.Log.Info("reservation compensated", "booking_id", id)
		}
	} else {
		b, e := scanBooking(tx.QueryRow(ctx, `SELECT `+bookingColumns+` FROM bookings WHERE id=$1 FOR UPDATE`, id))
		if e != nil {
			return s.dbError(e)
		}
		if b.Status == "CONFIRMED" {
			if _, err = s.Flights.ReleaseReservation(ctx, &pb.ReleaseReservationRequest{BookingId: id}); err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, `UPDATE bookings SET status='CANCELLED' WHERE id=$1`, id); err != nil {
				return s.dbError(err)
			}
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE booking_operations SET state='DONE' WHERE booking_id=$1 AND kind=$2`, id, kind); err != nil {
		return s.dbError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return s.dbError(err)
	}
	return nil
}

// Recover processes persisted intents. CREATE gets a grace period so a live
// handler can acquire its row lock; CANCEL/COMPENSATING are ready immediately.
func (s *Service) Recover(ctx context.Context) error {
	rows, err := s.DB.Query(ctx, `SELECT booking_id::text,kind FROM booking_operations WHERE state<>'DONE'
 AND (kind='CANCEL' OR state='COMPENSATING' OR created_at < now()-interval '30 seconds') ORDER BY created_at LIMIT 20`)
	if err != nil {
		return err
	}
	type op struct{ id, kind string }
	var ops []op
	for rows.Next() {
		var o op
		if err = rows.Scan(&o.id, &o.kind); err != nil {
			rows.Close()
			return err
		}
		ops = append(ops, o)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, o := range ops {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		callCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		err = s.recoverOne(callCtx, o.id, o.kind)
		cancel()
		if err != nil {
			s.Log.Warn("operation recovery failed; will retry", "booking_id", o.id, "kind", o.kind, "error", err)
		}
	}
	return nil
}

func (s *Service) RunRecovery(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Recover(ctx); err != nil && ctx.Err() == nil {
				s.Log.Error("recovery scan failed", "error", err)
			}
		}
	}
}
