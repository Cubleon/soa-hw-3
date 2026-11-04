package booking

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/mail"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	pb "flightbooking/gen/flight/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeProto(w http.ResponseWriter, v proto.Message) {
	b, err := (protojson.MarshalOptions{UseProtoNames: true, EmitDefaultValues: true}).Marshal(v)
	if err != nil {
		writeError(w, status.Error(codes.Internal, "response encoding failed"))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(b)
}

func writeError(w http.ResponseWriter, err error) {
	s := status.Convert(err)
	code := http.StatusInternalServerError
	switch s.Code() {
	case codes.InvalidArgument:
		code = http.StatusBadRequest
	case codes.NotFound:
		code = http.StatusNotFound
	case codes.AlreadyExists, codes.ResourceExhausted, codes.FailedPrecondition, codes.Aborted:
		code = http.StatusConflict
	case codes.Unavailable:
		code = http.StatusServiceUnavailable
		w.Header().Set("Retry-After", "10")
	case codes.DeadlineExceeded:
		code = http.StatusGatewayTimeout
	case codes.Canceled:
		code = http.StatusRequestTimeout
	case codes.Unauthenticated, codes.PermissionDenied:
		code = http.StatusBadGateway // Internal service credentials, not client auth.
	}
	message := s.Message()
	if s.Code() == codes.Unknown {
		message = "internal error"
	}
	writeJSON(w, code, map[string]any{"error": map[string]string{"code": s.Code().String(), "message": message}})
}

func validID(id string) bool {
	u, err := uuid.Parse(id)
	return err == nil && u != uuid.Nil && u.String() == id
}

var iata = regexp.MustCompile(`^[A-Z]{3}$`)

func validateCreate(r CreateRequest) error {
	bad := func(s string) error { return status.Error(codes.InvalidArgument, s) }
	if !validID(r.FlightID) {
		return bad("flight_id must be a canonical nonzero UUID")
	}
	if strings.TrimSpace(r.UserID) == "" || utf8.RuneCountInString(r.UserID) > 128 {
		return bad("user_id must contain 1..128 characters")
	}
	if strings.TrimSpace(r.PassengerName) == "" || utf8.RuneCountInString(r.PassengerName) > 200 {
		return bad("passenger_name must contain 1..200 characters")
	}
	a, err := mail.ParseAddress(r.PassengerEmail)
	if err != nil || a.Address != r.PassengerEmail || len(r.PassengerEmail) > 254 {
		return bad("passenger_email must be an email address")
	}
	if r.SeatCount <= 0 {
		return bad("seat_count must be positive")
	}
	return nil
}

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := s.DB.Ping(ctx); err != nil {
			writeJSON(w, 503, map[string]string{"status": "unhealthy"})
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /flights", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		origin, destination := q.Get("origin"), q.Get("destination")
		if !iata.MatchString(origin) || !iata.MatchString(destination) || origin == destination {
			writeError(w, status.Error(codes.InvalidArgument, "different origin and destination IATA codes are required"))
			return
		}
		req := &pb.SearchFlightsRequest{Origin: origin, Destination: destination}
		if date := q.Get("date"); date != "" {
			d, err := time.Parse(time.DateOnly, date)
			if err != nil {
				writeError(w, status.Error(codes.InvalidArgument, "date must be YYYY-MM-DD"))
				return
			}
			req.DepartureDate = timestamppb.New(d)
		}
		out, err := s.Flights.SearchFlights(r.Context(), req)
		if err != nil {
			writeError(w, err)
			return
		}
		writeProto(w, out)
	})
	mux.HandleFunc("GET /flights/{id}", func(w http.ResponseWriter, r *http.Request) {
		if !validID(r.PathValue("id")) {
			writeError(w, status.Error(codes.InvalidArgument, "id must be a UUID"))
			return
		}
		out, err := s.Flights.GetFlight(r.Context(), &pb.GetFlightRequest{Id: r.PathValue("id")})
		if err != nil {
			writeError(w, err)
			return
		}
		writeProto(w, out)
	})
	mux.HandleFunc("POST /bookings", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var req CreateRequest
		if err := decoder.Decode(&req); err != nil {
			writeError(w, status.Error(codes.InvalidArgument, "invalid JSON request"))
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			writeError(w, status.Error(codes.InvalidArgument, "expected a single JSON object"))
			return
		}
		if err := validateCreate(req); err != nil {
			writeError(w, err)
			return
		}
		out, err := s.Create(r.Context(), req)
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Location", "/bookings/"+out.ID)
		writeJSON(w, http.StatusCreated, out)
	})
	mux.HandleFunc("GET /bookings/{id}", func(w http.ResponseWriter, r *http.Request) {
		if !validID(r.PathValue("id")) {
			writeError(w, status.Error(codes.InvalidArgument, "id must be a UUID"))
			return
		}
		out, err := s.Get(r.Context(), r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, 200, out)
	})
	mux.HandleFunc("GET /bookings", func(w http.ResponseWriter, r *http.Request) {
		user := r.URL.Query().Get("user_id")
		if strings.TrimSpace(user) == "" || utf8.RuneCountInString(user) > 128 {
			writeError(w, status.Error(codes.InvalidArgument, "user_id is required (1..128 characters)"))
			return
		}
		out, err := s.List(r.Context(), user)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, 200, map[string]any{"bookings": out})
	})
	mux.HandleFunc("POST /bookings/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		if !validID(r.PathValue("id")) {
			writeError(w, status.Error(codes.InvalidArgument, "id must be a UUID"))
			return
		}
		out, err := s.Cancel(r.Context(), r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, 200, out)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
		s.Log.Info("http request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(start))
	})
}
