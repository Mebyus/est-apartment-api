.PHONY: build
build: tidy download
	go build -o bin/retranslator ./cmd/omp-demo-api

.PHONY: generate
generate: install-mockgen
	go generate ./...

.PHONY: install-mockgen
install-mockgen:
	go install github.com/golang/mock/mockgen@latest

.PHONY: test
test: tidy download generate
	go test -v ./...

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: download
download:
	go mod download