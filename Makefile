NAME := server
MAIN := cmd/server/main.go

PKG := `go list -mod=mod -f {{.Dir}} ./...`

all: build
init: mod-tidy install-gci install-lint

run: lint build
	@echo "Starting app..."
	./bin/$(NAME) $(RUNFLAGS) start

.PHONY: build
build:
	@mkdir -p bin
	@go build -mod=mod -o bin/$(NAME) $(MAIN)

mod-tidy:
	go mod tidy

mod-download:
	go mod download all

install-gci:
	go install github.com/daixiang0/gci@latest

install-lint:
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin latest

pre-commit: lint test

fmt:
	golangci-lint fmt ./...
	gci write -s standard -s default -s "Prefix(github.com/Blackdeer1524/GraphDB)" -s blank -s dot $(PKG)

lint: fmt
	golangci-lint run -E wsl -E bodyclose -E errname

.PHONY: test
test:
	go test ./... -count=1 -p=1
