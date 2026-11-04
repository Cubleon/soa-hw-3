CREATE TABLE bookings (
    id uuid PRIMARY KEY,
    user_id text NOT NULL CHECK (length(user_id) BETWEEN 1 AND 128),
    flight_id uuid NOT NULL,
    passenger_name text NOT NULL CHECK (length(passenger_name) BETWEEN 1 AND 200),
    passenger_email text NOT NULL CHECK (length(passenger_email) BETWEEN 3 AND 254),
    seat_count integer NOT NULL CHECK (seat_count > 0),
    -- Price snapshot belongs to the booking; it is independent of today's fare.
    total_price_minor bigint NOT NULL CHECK (total_price_minor > 0),
    currency char(3) NOT NULL DEFAULT 'RUB' CHECK (currency = 'RUB'),
    status text NOT NULL CHECK (status IN ('CONFIRMED','CANCELLED')),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX bookings_user ON bookings(user_id, created_at DESC);
-- Durable saga journal. payload is an immutable command snapshot, not a live
-- relational copy. No cross-database FK: flight_id is validated through gRPC.
CREATE TABLE booking_operations (
    booking_id uuid NOT NULL,
    kind text NOT NULL CHECK (kind IN ('CREATE','CANCEL')),
    state text NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING','COMPENSATING','DONE')),
    payload jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (booking_id, kind)
);
CREATE INDEX operations_pending ON booking_operations(created_at) WHERE state <> 'DONE';
