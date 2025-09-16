NAME := server
MAIN := cmd/server/main.go

GO            := go
GOBIN ?= $(shell $(GO) env GOPATH)/bin

PROTO_DIR     := api/proto
PROTO_FILES   := $(PROTO_DIR)/raft.proto
GEN_OUT_DIR   := src/generated/proto

PROTOC ?= protoc
PROTOC_GEN_GO         := $(GOBIN)/protoc-gen-go
PROTOC_GEN_GO_GRPC    := $(GOBIN)/protoc-gen-go-grpc

PKG := `go list -mod=mod -f {{.Dir}} ./...`

all: gen build
init: mod-tidy install-gci install-lint

run: build
	@echo "Starting app..."
	./bin/$(NAME) $(RUNFLAGS) start

mockery:
	@find . -type f -name 'mock_*' | xargs rm
	@mockery

.PHONY: build
build: init mockery
	@mkdir -p bin
	@go build -mod=mod -o bin/$(NAME) $(MAIN)

.PHONY: build-fast
build-fast:
	@mkdir -p bin
	@go build -mod=mod -o bin/$(NAME) $(MAIN)

gen: ## Generate gRPC + Protobuf code
	@command -v $(PROTOC) >/dev/null || (echo "protoc not found. Install protobuf compiler." && exit 1)
	@command -v $(PROTOC_GEN_GO) >/dev/null || (echo "protoc-gen-go not found. Run 'make tools'." && exit 1)
	@command -v $(PROTOC_GEN_GO_GRPC) >/dev/null || (echo "protoc-gen-go-grpc not found. Run 'make tools'." && exit 1)
	@mkdir -p $(GEN_OUT_DIR)
	$(PROTOC) -I $(PROTO_DIR) \
		--go_out=$(GEN_OUT_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_OUT_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

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
