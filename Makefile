VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
TEST_DB ?= postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable
TOKEN ?= demo-token-change-me-0123456789abcdef

.PHONY: test cover lint vuln build up down loadtest reconcile

test: ## Unit tests, plus Postgres tests when the database is up (make up)
	LEDGER_TEST_DATABASE_URL=$(TEST_DB) go test -race ./...

cover:
	LEDGER_TEST_DATABASE_URL=$(TEST_DB) go test -coverprofile=cover.out ./... && go tool cover -html=cover.out

lint:
	test -z "$$(gofmt -l .)" && go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.10.0 ./...

build:
	CGO_ENABLED=0 go build -ldflags "-X main.version=$(VERSION)" -o bin/ledger ./cmd/ledger
	CGO_ENABLED=0 go build -o bin/loadtest ./cmd/loadtest

up: ## Start Postgres, the ledger and an event receiver
	docker compose up --build -d --wait

down:
	docker compose down -v

loadtest: ## 30s of concurrent transfers, then check the books balance
	go run ./cmd/loadtest -url http://localhost:8080 -token $(TOKEN) -duration 30s

reconcile:
	docker compose exec ledger /ledger reconcile
