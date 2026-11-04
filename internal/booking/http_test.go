package booking

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHTTPValidationDoesNotReachDependencies(t *testing.T) {
	s := &Service{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for _, tc := range []struct{ method, path, body string }{
		{"GET", "/flights?origin=SVO", ""},
		{"GET", "/flights?origin=SVO&destination=SVO", ""},
		{"GET", "/flights?origin=SVO&destination=LED&date=2027-02-30", ""},
		{"GET", "/flights/bad", ""},
		{"GET", "/bookings/bad", ""},
		{"GET", "/bookings", ""},
		{"POST", "/bookings/bad/cancel", ""},
		{"POST", "/bookings", `{"unknown":true}`},
		{"POST", "/bookings", `{} {}`},
		{"POST", "/bookings", `{"flight_id":"11111111-1111-4111-8111-111111111111","user_id":"u","passenger_name":"Test","passenger_email":"bad","seat_count":1}`},
		{"POST", "/bookings", `{"flight_id":"11111111-1111-4111-8111-111111111111","user_id":"u","passenger_name":"Test","passenger_email":"a@example.com","seat_count":0}`},
	} {
		t.Run(tc.method+tc.path+tc.body, func(t *testing.T) {
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
			if w.Code != 400 {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			var out map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHTTPErrorMapping(t *testing.T) {
	for code, want := range map[codes.Code]int{codes.InvalidArgument: 400, codes.NotFound: 404, codes.ResourceExhausted: 409, codes.FailedPrecondition: 409, codes.Unavailable: 503, codes.DeadlineExceeded: 504, codes.Unauthenticated: 502, codes.Internal: 500} {
		w := httptest.NewRecorder()
		writeError(w, status.Error(code, "test"))
		if w.Code != want {
			t.Fatalf("%s: got %d want %d", code, w.Code, want)
		}
	}
}
