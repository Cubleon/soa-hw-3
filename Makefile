.PHONY: up down test integration generate fmt vet
up:
	docker compose up --build -d --wait
down:
	docker compose down
test:
	go test -race ./...
integration:
	docker compose --profile test run --build --rm integration
generate:
	protoc -I api --go_out=. --go_opt=module=flightbooking --go-grpc_out=. --go-grpc_opt=module=flightbooking api/flight/v1/flight.proto
fmt:
	gofmt -w cmd internal tests
vet:
	go vet ./...
