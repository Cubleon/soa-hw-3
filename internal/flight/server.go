package flight

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	pb "flightbooking/gen/flight/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	pb.UnimplementedFlightServiceServer
	db    *pgxpool.Pool
	cache *Cache
	log   *slog.Logger
}

func NewServer(db *pgxpool.Pool, cache *Cache, log *slog.Logger) *Server {
	return &Server{db: db, cache: cache, log: log}
}

var iata = regexp.MustCompile(`^[A-Z]{3}$`)

func validID(id string) bool {
	u, err := uuid.Parse(id)
	return err == nil && u != uuid.Nil && u.String() == id
}

func (s *Server) dbError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return status.Error(codes.NotFound, "flight not found")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	s.log.Error("flight database error", "error", err)
	return status.Error(codes.Internal, "flight storage error")
}

const flightSelect = `SELECT f.id::text, f.flight_number, f.airline_code, a.name, f.origin, f.destination,
 f.departure_at, f.arrival_at, f.total_seats, f.available_seats, f.price_minor, f.currency, f.status
 FROM flights f JOIN airlines a ON a.code=f.airline_code `

func scanFlight(row pgx.Row) (*pb.Flight, error) {
	f := &pb.Flight{}
	var departure, arrival time.Time
	var state string
	err := row.Scan(&f.Id, &f.FlightNumber, &f.AirlineCode, &f.AirlineName, &f.Origin, &f.Destination,
		&departure, &arrival, &f.TotalSeats, &f.AvailableSeats, &f.PriceMinor, &f.Currency, &state)
	if err != nil {
		return nil, err
	}
	f.DepartureAt = timestamppb.New(departure)
	f.ArrivalAt = timestamppb.New(arrival)
	f.Status = pb.FlightStatus(pb.FlightStatus_value[state])
	return f, nil
}

func (s *Server) snapshot(ctx context.Context) (pgx.Tx, int64, error) {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, 0, err
	}
	var generation int64
	if err = tx.QueryRow(ctx, `SELECT generation FROM cache_generation WHERE singleton`).Scan(&generation); err != nil {
		_ = tx.Rollback(ctx)
		return nil, 0, err
	}
	return tx, generation, nil
}

func (s *Server) GetFlight(ctx context.Context, r *pb.GetFlightRequest) (*pb.Flight, error) {
	if !validID(r.GetId()) {
		return nil, status.Error(codes.InvalidArgument, "id must be a canonical nonzero UUID")
	}
	tx, gen, err := s.snapshot(ctx)
	if err != nil {
		return nil, s.dbError(err)
	}
	defer tx.Rollback(context.Background())
	key := fmt.Sprintf("v%d:flight:%s", gen, r.Id)
	cached := &pb.Flight{}
	if s.cache.get(ctx, key, cached) {
		return cached, nil
	}
	f, err := scanFlight(tx.QueryRow(ctx, flightSelect+` WHERE f.id=$1`, r.Id))
	if err != nil {
		return nil, s.dbError(err)
	}
	s.cache.set(ctx, key, f)
	return f, nil
}

func (s *Server) SearchFlights(ctx context.Context, r *pb.SearchFlightsRequest) (*pb.SearchFlightsResponse, error) {
	if !iata.MatchString(r.GetOrigin()) || !iata.MatchString(r.GetDestination()) || r.Origin == r.Destination {
		return nil, status.Error(codes.InvalidArgument, "different origin and destination IATA codes are required")
	}
	var day any
	dateKey := "all"
	if r.DepartureDate != nil {
		if r.DepartureDate.CheckValid() != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid departure_date")
		}
		d := r.DepartureDate.AsTime().UTC()
		if d.Hour() != 0 || d.Minute() != 0 || d.Second() != 0 || d.Nanosecond() != 0 {
			return nil, status.Error(codes.InvalidArgument, "departure_date must be midnight UTC")
		}
		day = d
		dateKey = d.Format(time.DateOnly)
	}
	tx, gen, err := s.snapshot(ctx)
	if err != nil {
		return nil, s.dbError(err)
	}
	defer tx.Rollback(context.Background())
	key := fmt.Sprintf("v%d:search:%s:%s:%s", gen, r.Origin, r.Destination, dateKey)
	out := &pb.SearchFlightsResponse{Flights: []*pb.Flight{}}
	if s.cache.get(ctx, key, out) {
		return out, nil
	}
	rows, err := tx.Query(ctx, flightSelect+` WHERE f.origin=$1 AND f.destination=$2 AND f.status='SCHEDULED'
 AND ($3::timestamptz IS NULL OR (f.departure_at >= $3 AND f.departure_at < $3 + interval '1 day')) ORDER BY f.departure_at,f.id`, r.Origin, r.Destination, day)
	if err != nil {
		return nil, s.dbError(err)
	}
	defer rows.Close()
	for rows.Next() {
		f, e := scanFlight(rows)
		if e != nil {
			return nil, s.dbError(e)
		}
		out.Flights = append(out.Flights, f)
	}
	if err = rows.Err(); err != nil {
		return nil, s.dbError(err)
	}
	s.cache.set(ctx, key, out)
	return out, nil
}

// Serialize Reserve and Release for the same booking, even before a row exists.
func lockBooking(ctx context.Context, tx pgx.Tx, id string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, id)
	return err
}

func reservation(ctx context.Context, tx pgx.Tx, id string) (*pb.SeatReservation, error) {
	r := &pb.SeatReservation{}
	var state string
	var created time.Time
	err := tx.QueryRow(ctx, `SELECT booking_id::text,flight_id::text,seat_count,status,created_at FROM seat_reservations WHERE booking_id=$1`, id).
		Scan(&r.BookingId, &r.FlightId, &r.SeatCount, &state, &created)
	if err != nil {
		return nil, err
	}
	r.Status = pb.ReservationStatus(pb.ReservationStatus_value[state])
	r.CreatedAt = timestamppb.New(created)
	return r, nil
}

func invalidate(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `UPDATE cache_generation SET generation=generation+1 WHERE singleton`)
	return err
}

func (s *Server) ReserveSeats(ctx context.Context, r *pb.ReserveSeatsRequest) (*pb.SeatReservation, error) {
	if !validID(r.GetFlightId()) || !validID(r.GetBookingId()) || r.GetSeatCount() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "flight_id, booking_id and positive seat_count are required")
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, s.dbError(err)
	}
	defer tx.Rollback(context.Background())
	if err = lockBooking(ctx, tx, r.BookingId); err != nil {
		return nil, s.dbError(err)
	}
	old, err := reservation(ctx, tx, r.BookingId)
	if err == nil {
		if old.FlightId != r.FlightId || old.SeatCount != r.SeatCount {
			return nil, status.Error(codes.AlreadyExists, "booking_id was used with other reservation parameters")
		}
		if old.Status != pb.ReservationStatus_ACTIVE {
			return nil, status.Error(codes.FailedPrecondition, "reservation is no longer active")
		}
		return old, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, s.dbError(err)
	}
	var fenced bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM cancellation_fences WHERE booking_id=$1)`, r.BookingId).Scan(&fenced); err != nil {
		return nil, s.dbError(err)
	}
	if fenced {
		return nil, status.Error(codes.FailedPrecondition, "booking was already released")
	}
	// Row lock prevents concurrent requests from selling the last seat twice.
	var available int32
	var state string
	var departure time.Time
	err = tx.QueryRow(ctx, `SELECT available_seats,status,departure_at FROM flights WHERE id=$1 FOR UPDATE`, r.FlightId).Scan(&available, &state, &departure)
	if err != nil {
		return nil, s.dbError(err)
	}
	if state != "SCHEDULED" || !departure.After(time.Now()) {
		return nil, status.Error(codes.FailedPrecondition, "flight is not open for reservations")
	}
	if available < r.SeatCount {
		return nil, status.Error(codes.ResourceExhausted, "not enough available seats")
	}
	if _, err = tx.Exec(ctx, `UPDATE flights SET available_seats=available_seats-$2 WHERE id=$1`, r.FlightId, r.SeatCount); err != nil {
		return nil, s.dbError(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO seat_reservations(booking_id,flight_id,seat_count,status) VALUES($1,$2,$3,'ACTIVE')`, r.BookingId, r.FlightId, r.SeatCount); err != nil {
		return nil, s.dbError(err)
	}
	if err = invalidate(ctx, tx); err != nil {
		return nil, s.dbError(err)
	}
	out, err := reservation(ctx, tx, r.BookingId)
	if err != nil {
		return nil, s.dbError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, s.dbError(err)
	}
	s.log.Info("cache invalidated", "operation", "reserve", "flight_id", r.FlightId)
	return out, nil
}

func (s *Server) ReleaseReservation(ctx context.Context, r *pb.ReleaseReservationRequest) (*pb.ReleaseReservationResponse, error) {
	if !validID(r.GetBookingId()) {
		return nil, status.Error(codes.InvalidArgument, "booking_id must be a UUID")
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, s.dbError(err)
	}
	defer tx.Rollback(context.Background())
	if err = lockBooking(ctx, tx, r.BookingId); err != nil {
		return nil, s.dbError(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO cancellation_fences(booking_id) VALUES($1) ON CONFLICT DO NOTHING`, r.BookingId); err != nil {
		return nil, s.dbError(err)
	}
	old, err := reservation(ctx, tx, r.BookingId)
	released := false
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, s.dbError(err)
	}
	if err == nil && old.Status == pb.ReservationStatus_ACTIVE {
		// Use the same flight row lock as ReserveSeats. The booking advisory lock
		// already protects the reservation itself from concurrent releases.
		if _, err = tx.Exec(ctx, `SELECT id FROM flights WHERE id=$1 FOR UPDATE`, old.FlightId); err != nil {
			return nil, s.dbError(err)
		}
		if _, err = tx.Exec(ctx, `UPDATE flights SET available_seats=available_seats+$2 WHERE id=$1`, old.FlightId, old.SeatCount); err != nil {
			return nil, s.dbError(err)
		}
		if _, err = tx.Exec(ctx, `UPDATE seat_reservations SET status='RELEASED' WHERE booking_id=$1`, r.BookingId); err != nil {
			return nil, s.dbError(err)
		}
		if err = invalidate(ctx, tx); err != nil {
			return nil, s.dbError(err)
		}
		released = true
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, s.dbError(err)
	}
	if released {
		s.log.Info("cache invalidated", "operation", "release", "booking_id", r.BookingId)
	}
	return &pb.ReleaseReservationResponse{Released: released}, nil
}

func (s *Server) UpdateFlight(ctx context.Context, r *pb.UpdateFlightRequest) (*pb.Flight, error) {
	if !validID(r.GetId()) || r.GetPriceMinor() <= 0 || r.GetStatus() < pb.FlightStatus_SCHEDULED || r.GetStatus() > pb.FlightStatus_COMPLETED {
		return nil, status.Error(codes.InvalidArgument, "id, positive price_minor and valid status are required")
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, s.dbError(err)
	}
	defer tx.Rollback(context.Background())
	var old string
	if err = tx.QueryRow(ctx, `SELECT status FROM flights WHERE id=$1 FOR UPDATE`, r.Id).Scan(&old); err != nil {
		return nil, s.dbError(err)
	}
	next := r.Status.String()
	allowed := old == next || (old == "SCHEDULED" && (next == "DEPARTED" || next == "CANCELLED")) || (old == "DEPARTED" && next == "COMPLETED")
	if !allowed {
		return nil, status.Error(codes.FailedPrecondition, "invalid flight status transition")
	}
	if _, err = tx.Exec(ctx, `UPDATE flights SET price_minor=$2,status=$3 WHERE id=$1`, r.Id, r.PriceMinor, next); err != nil {
		return nil, s.dbError(err)
	}
	if err = invalidate(ctx, tx); err != nil {
		return nil, s.dbError(err)
	}
	f, err := scanFlight(tx.QueryRow(ctx, flightSelect+` WHERE f.id=$1`, r.Id))
	if err != nil {
		return nil, s.dbError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, s.dbError(err)
	}
	s.log.Info("cache invalidated", "operation", "update", "flight_id", r.Id)
	return f, nil
}
