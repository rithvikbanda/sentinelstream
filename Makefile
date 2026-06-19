.PHONY: build test race bench vet run-server run-simulator run-replay \
        docker-build docker-up docker-down clean

build:
	go build -o bin/server ./cmd/server
	go build -o bin/simulator ./cmd/simulator
	go build -o bin/replay ./cmd/replay

vet:
	go vet ./...

test:
	go test ./...

race:
	go test -race ./...

bench:
	go test ./benchmarks/... -run=^$$ -bench=. -benchmem

run-server: build
	./bin/server

run-simulator: build
	./bin/simulator --target 127.0.0.1:9000 --protocol udp

run-replay: build
	./bin/replay --file events.jsonl

docker-build:
	docker compose build

docker-up:
	docker compose up --build

docker-down:
	docker compose down

clean:
	rm -rf bin
