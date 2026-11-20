RUST_DIR := ../fluerd
RUST_TARGET := $(RUST_DIR)/target/release
BIN_DIR := ./bin

# Go module
GO_MODULE := ://github.com

# Protobuf configuration
PROTO_DIR := pkg/common/proto
PROTO_OUT := pkg/common/configpb
PROTO_FILE := $(PROTO_DIR)/patroli.proto

# Ensure Go protobuf plugins are available
export PATH := $(PATH):$(shell go env GOPATH)/bin

.PHONY: all proto-gen rust-lib go-build run-bench run-collector sim-grpc sim-udp sim-wave test clean

all: run-bench

# 0. Generate Go protobuf & gRPC code
proto-gen:
	mkdir -p $(PROTO_OUT)
	protoc \
		-I $(PROTO_DIR) \
		--go_out=$(PROTO_OUT) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) \
		--go-grpc_opt=paths=source_relative \
		$(PROTO_FILE)

# 1. Build Rust shared library
rust-lib:
	cd $(RUST_DIR) && cargo build --release --lib

# 2. Build Go binaries (bench, collector, & simulator)
go-build: rust-lib
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=1 \
	CGO_LDFLAGS="-L$(abspath $(RUST_TARGET)) -lfluerd" \
	go build -o $(BIN_DIR)/bench ./cmd/bench
	CGO_ENABLED=1 \
	CGO_LDFLAGS="-L$(abspath $(RUST_TARGET)) -lfluerd" \
	go build -o $(BIN_DIR)/collector ./cmd/collector
	go build -o $(BIN_DIR)/simulator ./cmd/simulator

# 3. Run benchmark (1M frames in-memory FFI stress test)
run-bench: go-build
	LD_LIBRARY_PATH="$(abspath $(RUST_TARGET)):$$LD_LIBRARY_PATH" $(BIN_DIR)/bench

# 4. Run collector (UDP, HTTP, gRPC, Prometheus & UI)
run-collector: go-build
	LD_LIBRARY_PATH="$(abspath $(RUST_TARGET)):$$LD_LIBRARY_PATH" $(BIN_DIR)/collector

# 5. Simulator scenarios (jalankan di terminal kedua saat collector aktif)
sim-grpc:
	go run ./cmd/simulator -mode=grpc -workers=20 -total=100000

sim-udp:
	go run ./cmd/simulator -mode=udp -workers=50 -total=200000

sim-wave:
	go run ./cmd/simulator -mode=wave -workers=10 -total=2000

# 6. Run tests (Rust unit tests + Go FFI integration tests)
test: rust-lib
	cd $(RUST_DIR) && cargo test
	CGO_ENABLED=1 \
	CGO_LDFLAGS="-L$(abspath $(RUST_TARGET)) -lfluerd" \
	LD_LIBRARY_PATH="$(abspath $(RUST_TARGET)):$$LD_LIBRARY_PATH" \
	go test -v ./pkg/engine/...

# 7. Clean generated files and binaries
clean:
	rm -rf $(BIN_DIR) $(PROTO_OUT)
	cd $(RUST_DIR) && cargo clean
