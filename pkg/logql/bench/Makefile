.PHONY: generate bench list run run-debug stream

# Default size for data generation (2GB)
SIZE ?= 2147483648
TENANT ?= test-tenant

generate:
	@echo "Generating benchmark data..."
	@go run -tags slicelabels ./cmd/generate/main.go -size $(SIZE) -tenant $(TENANT)

bench:
	@echo "Running all benchmarks..."
	@go test -tags slicelabels -bench=. -benchmem -count=1 -timeout=1h

list:
	@go run -tags slicelabels ./cmd/bench/main.go list

run:
	@go run -tags slicelabels ./cmd/bench/main.go run

run-debug:
	@DEBUG=1 go run -tags slicelabels ./cmd/bench/main.go run

stream:
	@go run -tags slicelabels ./cmd/stream/main.go | less
