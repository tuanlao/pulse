# pulse — build, test, and integration-test targets.
#
# Unit tests run with no external dependencies. Integration tests are guarded by
# the `integration` build tag and need the docker-compose stack (redis, kafka,
# temporal) up. They read infra addresses from env vars (defaults to localhost):
#   KAFKA_BROKERS, REDIS_ADDR, TEMPORAL_HOSTPORT.

.PHONY: build vet test test-integration infra-up infra-down infra-logs

# Build everything.
build:
	go build ./...

vet:
	go vet ./...

# Unit tests (no infra, no integration tag).
test:
	go test -race -cover ./pkg/...

# Bring up the integration infra and wait until every service is healthy.
infra-up:
	docker compose up -d --wait

infra-down:
	docker compose down -v

infra-logs:
	docker compose logs -f

# Integration tests — requires `make infra-up` first (or an equivalent stack).
test-integration:
	go test -race -tags=integration ./... -timeout 5m
