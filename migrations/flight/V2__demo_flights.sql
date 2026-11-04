INSERT INTO airlines VALUES ('SU','Аэрофлот'), ('DP','Победа');
INSERT INTO airports VALUES ('SVO','Шереметьево'), ('LED','Пулково'), ('VKO','Внуково');
INSERT INTO flights(id, flight_number, airline_code, origin, destination, departure_at,
                    arrival_at, total_seats, available_seats, price_minor, status)
VALUES
('11111111-1111-4111-8111-111111111111','SU1000','SU','SVO','LED','2027-04-01 09:00:00+00','2027-04-01 10:30:00+00',100,100,550000,'SCHEDULED'),
('22222222-2222-4222-8222-222222222222','DP200','DP','VKO','LED','2027-04-01 12:00:00+00','2027-04-01 13:30:00+00',50,50,350000,'SCHEDULED'),
('33333333-3333-4333-8333-333333333333','SU1001','SU','LED','SVO','2027-04-02 09:00:00+00','2027-04-02 10:30:00+00',80,80,600000,'SCHEDULED');
