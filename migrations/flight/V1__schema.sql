CREATE TABLE airlines (
    code varchar(3) PRIMARY KEY CHECK (code ~ '^[A-Z0-9]{2,3}$'),
    name text NOT NULL CHECK (length(name) > 0)
);
CREATE TABLE airports (
    code char(3) PRIMARY KEY CHECK (code ~ '^[A-Z]{3}$'),
    name text NOT NULL CHECK (length(name) > 0)
);
CREATE TABLE flights (
    id uuid PRIMARY KEY,
    flight_number varchar(10) NOT NULL CHECK (length(flight_number) > 0),
    airline_code varchar(3) NOT NULL REFERENCES airlines(code),
    origin char(3) NOT NULL REFERENCES airports(code),
    destination char(3) NOT NULL REFERENCES airports(code),
    departure_at timestamptz NOT NULL,
    arrival_at timestamptz NOT NULL,
    total_seats integer NOT NULL CHECK (total_seats > 0),
    available_seats integer NOT NULL CHECK (available_seats BETWEEN 0 AND total_seats),
    price_minor bigint NOT NULL CHECK (price_minor > 0),
    currency char(3) NOT NULL DEFAULT 'RUB' CHECK (currency = 'RUB'),
    status text NOT NULL CHECK (status IN ('SCHEDULED','DEPARTED','CANCELLED','COMPLETED')),
    CHECK (origin <> destination),
    CHECK (arrival_at > departure_at)
);
-- The assignment defines uniqueness by UTC calendar date, not departure time.
CREATE UNIQUE INDEX flights_number_date ON flights(flight_number, ((departure_at AT TIME ZONE 'UTC')::date));
CREATE INDEX flights_search ON flights(origin, destination, departure_at) WHERE status = 'SCHEDULED';
CREATE TABLE seat_reservations (
    booking_id uuid PRIMARY KEY,
    flight_id uuid NOT NULL REFERENCES flights(id),
    seat_count integer NOT NULL CHECK (seat_count > 0),
    status text NOT NULL CHECK (status IN ('ACTIVE','RELEASED','EXPIRED')),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX reservations_flight ON seat_reservations(flight_id);
-- Release of an unknown booking must prevent a late ReserveSeats from creating
-- an orphan reservation after an ambiguous timeout. Rows are permanent fences.
CREATE TABLE cancellation_fences (
    booking_id uuid PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);
-- Transactional cache namespace: all readers first obtain the DB generation.
-- Rollback also rolls back invalidation; stale Redis replicas cannot resurrect
-- keys from an earlier committed generation.
CREATE TABLE cache_generation (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    generation bigint NOT NULL DEFAULT 1 CHECK (generation > 0)
);
INSERT INTO cache_generation DEFAULT VALUES;
