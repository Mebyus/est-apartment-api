.PHONY: build
build:
	go build -o bin/retranslator ./cmd/omp-demo-api

.PHONY: test
test:
	go test -v ./...