# ER-диаграмма и 3NF

Таблицы `airlines`, `airports`, `flights`, `seat_reservations`,
`cancellation_fences`, `cache_generation` принадлежат **Flight DB**.
`bookings`, `booking_operations` — **Booking DB**. Пунктир — логическая связь,
которая проверяется протоколом и не является внешним ключом между базами.

```mermaid
erDiagram
    AIRLINES ||--o{ FLIGHTS : operates
    AIRPORTS ||--o{ FLIGHTS : origin
    AIRPORTS ||--o{ FLIGHTS : destination
    FLIGHTS ||--o{ SEAT_RESERVATIONS : has
    BOOKINGS o|..|| SEAT_RESERVATIONS : "logical booking_id, no cross-DB FK"

    AIRLINES {
        varchar code PK
        text name
    }
    AIRPORTS {
        char code PK "IATA: 3 uppercase letters"
        text name
    }
    FLIGHTS {
        uuid id PK
        varchar flight_number "UNIQUE together with UTC departure date"
        varchar airline_code FK
        char origin FK
        char destination FK
        timestamptz departure_at
        timestamptz arrival_at "greater than departure_at"
        integer total_seats "greater than 0"
        integer available_seats "0..total_seats"
        bigint price_minor "greater than 0, kopecks"
        char currency "RUB"
        text status "SCHEDULED|DEPARTED|CANCELLED|COMPLETED"
    }
    SEAT_RESERVATIONS {
        uuid booking_id PK "one reservation per booking"
        uuid flight_id FK
        integer seat_count "greater than 0"
        text status "ACTIVE|RELEASED|EXPIRED"
        timestamptz created_at
    }
    CANCELLATION_FENCES {
        uuid booking_id PK "prevents late Reserve after Release"
        timestamptz created_at
    }
    CACHE_GENERATION {
        boolean singleton PK "always true"
        bigint generation "positive, monotonically increasing"
    }
    BOOKINGS {
        uuid id PK
        text user_id "1..128 characters"
        uuid flight_id "external ID via gRPC"
        text passenger_name "1..200 characters"
        text passenger_email
        integer seat_count "greater than 0"
        bigint total_price_minor "positive immutable snapshot"
        char currency "RUB"
        text status "CONFIRMED|CANCELLED"
        timestamptz created_at
    }
    BOOKING_OPERATIONS {
        uuid booking_id PK "composite key"
        text kind PK "CREATE|CANCEL"
        text state "PENDING|COMPENSATING|DONE"
        jsonb payload "immutable command snapshot"
        timestamptz created_at
    }
```

* Все значения предметных атрибутов атомарны, повторяющихся групп нет (1NF).
* Атрибуты зависят от целого первичного ключа. У журнала составной ключ
  `(booking_id, kind)`; состояние и команда относятся к конкретной операции (2NF).
* Названия авиакомпаний и аэропортов вынесены в справочники. Рейс хранит ссылки
  на них, поэтому отсутствуют зависимости `flight_id → airline_code → airline_name`
  внутри таблицы рейсов (3NF).
* В Booking DB не копируются маршрут, авиакомпания, время вылета и текущий тариф.
  Данные пассажира — атрибуты конкретного бронирования: один пользователь может
  бронировать билеты для разных пассажиров. `total_price_minor` — исторический
  снимок сделки, а не производная от текущей записи другой БД. Отдельная цена
  за место в бронировании не дублируется.
* JSONB журнала — сериализованный неизменяемый документ команды для восстановления;
  таблица журнала не используется как источник актуальных данных бронирования.
* `available_seats` — требуемый заданием счётчик агрегата. Его согласованность с
  ACTIVE-резервациями поддерживается единой транзакцией и блокировкой рейса.

Ограничения CHECK контролируют количество мест, положительную цену, статусы,
разные аэропорты, порядок времени и формат IATA. Уникальный индекс
`(flight_number, (departure_at AT TIME ZONE 'UTC')::date)` обеспечивает уникальность
номера рейса в пределах календарного дня без хранения производного столбца даты.
Внешние ключи внутри Flight DB обеспечивают целостность справочников и резерваций.
`booking_id` — PK резервации, поэтому дубликат невозможен на уровне БД.

Статус EXPIRED предусмотрен контрактом и схемой; автоматическое истечение
резерваций не входит в сценарии задания. ACTIVE сохраняется до отмены.
Служебная резервация после компенсации может существовать без Booking:
это RELEASED-запись аудита неуспешной попытки. Fence может существовать без
резервации, если отмена опередила поздний запрос ReserveSeats.
