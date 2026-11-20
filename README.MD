# fluerd — High-Throughput Telemetry Engine & Multi-Protocol Go-Rust Orchestrator

`fluerd` is a high-performance binary telemetry processing system built on a hybrid **Go + Rust architecture using `cdylib` FFI**.

It separates **Multi-Protocol Ingestion & Observability** on the Go side from a **Zero-Copy Data Plane & Parallel Compute Engine** on the Rust side.

The Go orchestrator handles gRPC/Protobuf, HTTP, UDP, Prometheus-compatible metrics, distributed trace visualization, and traffic simulation. The Rust core handles zero-copy binary parsing, bounded asynchronous message processing, deterministic batching, and parallel CPU computation using Tokio and Rayon.

The architecture minimizes unnecessary memory allocations, reduces garbage collection pressure, and provides predictable backpressure under high-throughput workloads.

---

## Table of Contents

- [End-to-End Architecture](#end-to-end-architecture)
- [Monorepo Directory Structure](#monorepo-directory-structure)
- [Benchmarks & Performance Characteristics](#benchmarks--performance-characteristics)
- [Engineering Trade-Offs](#engineering-trade-offs-latency-vs-throughput)
- [Technical Deep Dive & Design Decisions](#technical-deep-dive--design-decisions)
  - [Binary Wire Protocol](#1-binary-wire-protocol)
  - [Safe Go-Rust FFI Boundary](#2-safe-go-rust-ffi-boundary)
  - [Hybrid Batching & Parallel Compute](#3-deterministic-hybrid-batching--parallel-compute)
- [Getting Started](#getting-started)
- [Running the Collector](#1-run-the-multi-protocol-collector--web-ui)
- [Stress Testing & Simulation](#2-run-stress-tests--network-traffic-simulators)
- [Testing & Linting](#3-automated-testing--linting)

---

## End-to-End Architecture

```text
+-----------------------------------------------------------------------------+
|                      GO ORCHESTRATOR (orchestrator/)                        |
|                                                                             |
|  [gRPC :50051]       [HTTP :8081]        [UDP :9000]       [CLI Bench/Sim]  |
|   (Protobuf)         (POST /ingest)      (sync.Pool)        (1M+ Frames)   |
|       \                   |                  /                  /           |
|        +------------------+-----------------+------------------+            |
|                           |                                                 |
|                           v  Zero-Allocation Binary Wire Format             |
|              +--------------------------------------+                       |
|              | pkg/engine — Safe cgo FFI Wrapper   |                       |
|              |                                      |                       |
|              | - Lock-free atomic metrics           |                       |
|              | - W3C-compatible trace spans         |                       |
|              | - UI Console & Prometheus metrics    |                       |
|              | - Trace visualization (/traces)      |                       |
|              +--------------------------------------+                       |
|                           |                                                 |
+---------------------------+-------------------------------------------------+
                            |
                            | extern "C" FFI Boundary
                            | ~273 ns per call (benchmark result)
                            | catch_unwind + non-blocking try_send
                            v
+-----------------------------------------------------------------------------+
|                         RUST CORE ENGINE (fluerd/)                          |
|                                                                             |
|  1. ZERO-COPY GATEKEEPER (src/parser/mod.rs)                                |
|     - Slice pattern matching and split_at_checked                           |
|     - Borrowed byte slices (&[u8])                                          |
|     - Filters non-critical frames (severity < 7)                            |
|     - Avoids heap allocation during initial parsing                        |
|                            |                                                |
|                            v                                                |
|  2. BOUNDED CHANNEL BACKPRESSURE                                            |
|     - tokio::sync::mpsc::channel                                           |
|     - Non-blocking try_send                                                 |
|     - Sheds excess traffic safely                                           |
|     - Caps queued message memory usage                                      |
|                            |                                                |
|                            v  rx.recv_many()                                |
|  3. HYBRID BATCH CONSUMER (src/pipeline/mod.rs)                             |
|     - tokio::select!                                                        |
|     - Flushes when batch is full OR timeout expires                         |
|     - O(1) buffer ownership transfer                                        |
|     - std::mem::replace                                                     |
|                            |                                                |
|                            v  tokio::task::spawn_blocking                   |
|  4. PARALLEL CPU COMPUTE POOL                                               |
|     - Rayon parallel iterators                                              |
|     - batch.par_iter()                                                      |
|     - Overflow-safe saturating arithmetic                                   |
|                                                                             |
+-----------------------------------------------------------------------------+
```

### Core Responsibilities

| Component | Responsibility | Technology |
|---|---|---|
| Multi-Protocol Gateway | Network ingestion | Go, gRPC, HTTP, UDP |
| Binary Encoder | Compact telemetry serialization | Go |
| FFI Wrapper | Safe Go-Rust communication | cgo, C ABI |
| Zero-Copy Parser | Binary frame validation and filtering | Rust |
| Backpressure Layer | Bounded asynchronous message delivery | Tokio |
| Batch Consumer | Size-based and timeout-based flushing | Tokio |
| Compute Engine | Parallel telemetry processing | Rayon |
| Observability | Metrics, tracing, and dashboard | Native Go |

---

## Monorepo Directory Structure

```text
fluerd/
|
+-- fluerd/                                # Rust Core Engine
|   |
|   +-- Cargo.toml
|   |
|   +-- src/
|       |
|       +-- lib.rs                         # cdylib + rlib library entrypoint
|       |
|       +-- main.rs                        # Standalone Rust pipeline runner
|       |
|       +-- ffi/
|       |   +-- ffi.rs                     # extern "C" FFI boundary
|       |
|       +-- parser/
|       |   +-- mod.rs                     # Zero-copy binary parser
|       |
|       +-- pipeline/
|       |   +-- mod.rs                     # Hybrid batching and Rayon compute
|       |
|       +-- concepts/
|           +-- mod.rs                     # Synchronization pattern comparisons
|
+-- orchestrator/                          # Go Gateway & Observability
    |
    +-- Makefile                           # Build, test, benchmark automation
    |
    +-- go.mod
    |
    +-- cmd/
    |   |
    |   +-- collector/
    |   |   +-- main.go                    # Multi-protocol daemon
    |   |
    |   +-- bench/
    |   |   +-- main.go                    # 1M-frame FFI stress benchmark
    |   |
    |   +-- simulator/
    |       +-- main.go                    # Network traffic generator
    |
    +-- internal/
    |   |
    |   +-- shared/
    |   |   +-- telemetry/
    |   |       |
    |   |       +-- metrics.go             # Prometheus text exposition
    |   |       +-- tracer.go              # Trace span ring buffer
    |   |       +-- dashboard.go           # Interactive Web UI
    |   |
    |   +-- transport/
    |       |
    |       +-- transport.go               # FrameIngestor interface
    |       +-- grpc_server.go             # gRPC unary and streaming
    |       +-- http_server.go             # HTTP ingestion endpoint
    |       +-- udp_server.go              # UDP datagram listener
    |
    +-- pkg/
        |
        +-- common/
        |   |
        |   +-- proto/
        |   |   +-- patroli.proto          # Protocol Buffers v3 contract
        |   |
        |   +-- configpb/                  # Generated Protobuf bindings
        |
        +-- engine/
        |   |
        |   +-- engine.go                  # Safe Go FFI wrapper
        |   +-- engine_test.go             # FFI lifecycle integration tests
        |
        +-- protocol/
            +-- frame.go                   # Binary wire-format encoder
```

---

## Benchmarks & Performance Characteristics

Benchmark results obtained on Linux x86_64 with the following configuration:

| Configuration | Value |
|---|---|
| Bounded Channel Capacity | 10,000 |
| Batch Size | 512 frames |
| Flush Interval | 100 ms |
| FFI Architecture | Go + Rust cdylib |
| Parallel Compute | Rayon |
| Async Runtime | Tokio |

### Benchmark Results

| Benchmark Scenario | Traffic Load | Effective Throughput | Latency / Behavior |
|---|---:|---:|---|
| **In-Memory FFI Burst** (`make run-bench`) | 1,000,000 frames / 100 goroutines | **~16.29M frames/sec** | ~0.273 µs average FFI call |
| **gRPC Client Streaming** (`make sim-grpc`) | 100,000 messages / 20 streams | **~2.74M frames/sec** | 36.4 ms total elapsed |
| **UDP Datagram Flood** (`make sim-udp`) | 200,000 packets / 50 workers | **~1.28M frames/sec** | 155.7 ms total elapsed |
| **Wave Traffic Simulation** (`make sim-wave`) | 5 waves × 400 packets | Deterministic timeout flushing | Validates partial batch flushing |
| **Collector Memory Footprint** | 4 active servers + Web UI | **Less than 3 MB Go heap** | Reported runtime heap footprint |

### Performance Highlights

**1. In-Memory FFI Throughput**

- Processes a benchmark workload of one million telemetry frames.
- Uses 100 concurrent Go goroutines.
- Measures approximately 273 ns average FFI call latency.
- Performs early filtering without allocating owned packet structures for non-critical frames.
- Uses bounded queues to prevent unbounded memory growth.

**2. gRPC Streaming Performance**

- Handles 100,000 Protobuf messages.
- Uses 20 concurrent client streams.
- Achieves approximately 2.74 million frames per second in the reported local benchmark.

**3. UDP Ingestion Performance**

- Processes 200,000 binary datagrams.
- Uses 50 concurrent workers.
- Reuses buffers through Go's `sync.Pool`.
- Achieves approximately 1.28 million frames per second.

**4. Deterministic Timeout Flushing**

- Sends five traffic waves.
- Each wave contains 400 packets.
- Introduces a 200 ms idle interval between waves.
- Verifies that partial batches are flushed even when the batch size has not reached 512.

**5. Memory Efficiency**

- Reported Go heap footprint below 3 MB during the tested collector workload.
- Reduces unnecessary Go-side object allocation.
- Uses bounded asynchronous queues and reusable buffers.

*Note: These figures represent observed benchmark results under the stated test configuration, not universal performance guarantees. Results depend on CPU architecture, compiler versions, workload characteristics, operating system, and runtime configuration. Go heap measurements should also be distinguished from total process RSS and Rust-side memory consumption.*

---

## Engineering Trade-Offs (Latency vs. Throughput)

The system deliberately balances low-latency ingestion against high-throughput batch processing.

### Sub-Microsecond Ingress Gatekeeping

The Rust parser performs:

- Binary frame validation.
- Severity extraction.
- Device ID boundary checking.
- Non-critical packet filtering.
- Borrowed-slice processing.

Non-critical frames with severity below 7 are discarded before entering the expensive processing pipeline.

This minimizes unnecessary memory allocation and reduces downstream processing pressure.

### Throughput-Oriented Batching

The compute pipeline aggregates incoming telemetry into batches of up to 512 frames.

A batch is flushed when:

1. The batch reaches its configured capacity.
2. The flush timeout expires.

This approach amortizes scheduling and processing overhead across multiple frames.

Rayon's parallel iterator then distributes CPU-bound processing across available worker threads.

### Bounded Backpressure

The asynchronous channel uses a fixed capacity of 10,000 messages.

When incoming traffic exceeds available processing capacity, `try_send()` returns immediately rather than blocking the calling Go goroutine.

This provides predictable memory usage and prevents uncontrolled queue growth.

---

## Technical Deep Dive & Design Decisions

### 1. Binary Wire Protocol

**Implementation:**
- `orchestrator/pkg/protocol/frame.go`
- `fluerd/src/parser/mod.rs`

The telemetry engine uses a compact binary wire format to minimize serialization overhead.

#### Frame Layout

```text
+---------------+-------------------+-----------------------+--------------------+
| Severity (1B) | ID Length (2B BE)  | Device ID (N Bytes)   | Payload (Rest)     |
+---------------+-------------------+-----------------------+--------------------+
```

| Field | Size | Description |
|---|---:|---|
| Severity | 1 byte | Telemetry severity level |
| ID Length | 2 bytes | Big-endian device ID length |
| Device ID | N bytes | Variable-length device identifier |
| Payload | Remaining bytes | Raw telemetry payload |

#### Zero-Copy Parsing

The parser exposes a borrowed packet representation:

```rust
parse_raw_bytes(
    input: &[u8]
) -> Result<ZeroCopyPacket<'_>, ParseError>
```

Instead of immediately allocating new buffers, the parser borrows slices directly from the original input.

This enables:

- Reduced heap allocation.
- Lower memory copying overhead.
- Explicit lifetime management.
- Faster packet inspection.
- Efficient filtering before ownership transfer.

Only critical packets with severity greater than or equal to 7 are promoted into owned telemetry structures when required by the downstream processing pipeline.

#### Panic-Free Slicing

The parser uses safe slice operations and checked boundaries.

For example:

```rust
let (device_id, payload) = input
    .split_at_checked(id_len)
    .ok_or(ParseError::PacketTooShort)?;
```

Malformed or truncated frames return an explicit parsing error instead of causing an out-of-bounds panic.

---

### 2. Safe Go-Rust FFI Boundary

**Implementation:**
- `fluerd/src/ffi/ffi.rs`
- `orchestrator/pkg/engine/engine.go`

The system uses Rust's `cdylib` compilation target to expose a C-compatible interface to Go through cgo.

#### Non-Blocking FFI

The FFI boundary uses Tokio's `try_send()` instead of an asynchronous blocking send operation.

```rust
match tx.try_send(packet) {
    Ok(()) => STATUS_OK,
    Err(TrySendError::Full(_)) => STATUS_BACKPRESSURE,
    Err(TrySendError::Closed(_)) => STATUS_CLOSED,
}
```

This is important because cgo calls execute across the Go-Rust boundary synchronously.

Blocking inside the FFI entrypoint could unnecessarily occupy an operating-system thread.

The non-blocking design provides immediate feedback when the Rust processing queue is full.

#### Panic Boundary Protection

Every exported C ABI entrypoint protects against Rust panics using:

```rust
std::panic::catch_unwind(|| {
    // FFI operation
})
```

Rust stack unwinding must never cross the C ABI boundary.

The panic guard converts unexpected Rust panics into controlled error statuses instead of allowing unwinding into the Go runtime.

*Note: `catch_unwind` requires an appropriate panic strategy, typically `panic = "unwind"`, and does not recover from process aborts.*

#### Explicit Memory Ownership

The architecture follows a clear ownership contract:

1. Go owns the incoming byte slice.
2. Go guarantees that the input memory remains valid during the synchronous FFI call.
3. Rust borrows the input only during that call.
4. Critical telemetry frames are copied into Rust-owned memory before being queued for asynchronous processing.
5. Rust never retains a pointer to Go-managed memory after the FFI call returns.

This avoids unsafe cross-runtime memory ownership and prevents asynchronous access to expired Go buffers.

---

### 3. Deterministic Hybrid Batching & Parallel Compute

**Implementation:**
- `fluerd/src/pipeline/mod.rs`

The Rust pipeline combines asynchronous ingestion with parallel CPU-bound processing.

#### Batch Channel Reads

The consumer uses Tokio's `recv_many()` to drain multiple messages from the bounded channel in a single operation.

```rust
rx.recv_many(
    &mut batch_buffer,
    remaining_cap
).await;
```

This reduces repeated channel operations and improves ingestion efficiency under high traffic.

#### Resettable Timeout

The pipeline uses `tokio::select!` to coordinate batch capacity and timeout-based flushing.

The timer is configured with:

```rust
MissedTickBehavior::Delay
```

The timeout window is associated with the first message entering an empty batch.

When the batch transitions from empty to non-empty, the timeout is reset so that every partial batch receives a full flush window.

This avoids premature flushing caused by an interval timer that started before the first packet arrived.

#### O(1) Buffer Ownership Transfer

Once a batch is ready, the pipeline transfers ownership of its buffer using:

```rust
let ready_batch = std::mem::replace(
    &mut batch_buffer,
    Vec::with_capacity(BATCH_SIZE)
);
```

This avoids copying the entire batch during handoff.

The newly allocated replacement buffer becomes the active ingestion buffer while the previous buffer is processed independently.

#### Tokio and Rayon Integration

CPU-bound computation is offloaded from Tokio's asynchronous worker pool:

```rust
tokio::task::spawn_blocking(move || {
    ready_batch
        .par_iter()
        .map(|packet| {
            // Parallel telemetry scoring
        })
        .collect::<Vec<_>>()
});
```

Rayon's `par_iter()` distributes work across available CPU cores.

This separation prevents expensive CPU-bound processing from monopolizing Tokio's asynchronous workers.

#### Overflow-Safe Arithmetic

The compute pipeline uses saturating arithmetic where appropriate:

```rust
let weighted_score = score.saturating_mul(weight);
```

This prevents integer overflow from unexpectedly wrapping values during telemetry scoring.

---

## Getting Started

### Prerequisites

Make sure the following dependencies are installed.

| Dependency | Minimum Version | Purpose |
|---|---|---|
| Rust | 1.80+ | Core processing engine |
| Cargo | Included with Rust | Build and dependency management |
| Go | 1.22+ | Multi-protocol orchestrator |
| GCC or Clang | Compatible C compiler | cgo FFI compilation |
| Protocol Buffers | Compatible `protoc` | Protobuf code generation |
| protoc-gen-go | Compatible version | Go Protobuf bindings |
| protoc-gen-go-grpc | Compatible version | Go gRPC bindings |

Enable cgo:

```bash
export CGO_ENABLED=1
```

Verify the installed toolchains:

```bash
rustc --version
cargo --version
go version
gcc --version
protoc --version
```

---

## 1. Run the Multi-Protocol Collector & Web UI

Navigate to the Go orchestrator directory:

```bash
cd orchestrator
```

Build and start the collector:

```bash
make run-collector
```

The collector exposes the following services:

| Service | Address | Description |
|---|---|---|
| gRPC | `localhost:50051` | Protobuf unary and client-streaming ingestion |
| HTTP | `localhost:8081` | HTTP POST binary ingestion |
| UDP | `localhost:9000` | High-frequency binary datagram ingestion |
| Web UI | `localhost:9090` | Interactive telemetry dashboard |
| Prometheus | `localhost:9090/metrics` | Metrics exposition |
| Trace Viewer | `localhost:9090/traces` | Jaeger-style trace span visualization |

### Web Interfaces

Open the following endpoints in your browser:

**Interactive Dashboard**

```text
http://localhost:9090/
```

**Prometheus Metrics**

```text
http://localhost:9090/metrics
```

**Trace Visualization Data**

```text
http://localhost:9090/traces
```

---

## 2. Run Stress Tests & Network Traffic Simulators

The project includes multiple benchmark and simulation commands.

### In-Memory FFI Benchmark

Run the one-million-frame stress test:

```bash
make run-bench
```

This benchmark measures:

- Go-to-Rust FFI call overhead.
- Concurrent goroutine ingestion.
- Rust-side packet filtering.
- Bounded channel behavior.
- Overall frame processing throughput.

### gRPC Client-Streaming Simulation

In another terminal, while the collector is running:

```bash
make sim-grpc
```

Generates:

- 100,000 Protobuf messages.
- 20 concurrent client streams.
- HTTP/2-based local ingestion traffic.

### UDP Datagram Flood Simulation

```bash
make sim-udp
```

Generates:

- 200,000 binary UDP datagrams.
- 50 concurrent workers.
- High-frequency network ingestion traffic.

### Wave Traffic Simulation

```bash
make sim-wave
```

Generates five traffic waves with 400 packets per wave and 200 ms idle intervals.

This specifically validates the hybrid batching mechanism:

- Partial batches are flushed during idle periods.
- Timeout behavior remains deterministic.
- The pipeline does not require the batch to reach 512 frames before processing.

---

## 3. Automated Testing & Linting

The project provides a unified Makefile target for Rust and Go testing.

### Run All Tests

From the `orchestrator/` directory:

```bash
make test
```

This runs the configured Rust unit tests and Go FFI integration tests.

### Run Rust Unit Tests Independently

From the repository root:

```bash
cd fluerd
cargo test
```

### Run Go Integration Tests Independently

From the orchestrator directory:

```bash
cd orchestrator
go test ./...
```

### Strict Rust Clippy Audit

Run Clippy with Pedantic linting:

```bash
cd fluerd

cargo clippy --all-targets -- -W clippy::pedantic
```

The goal is to maintain:

- Memory-safe Rust abstractions.
- Explicit error handling.
- Clean FFI boundaries.
- Correct ownership and lifetime management.
- Consistent concurrency patterns.
- Minimal unnecessary allocations.

---

## Core Technologies

| Technology | Role |
|---|---|
| **Go** | Multi-protocol gateway and orchestration |
| **Rust** | High-performance binary processing |
| **cgo** | Go-Rust interoperability |
| **C ABI** | Stable FFI interface |
| **Tokio** | Asynchronous runtime and bounded channels |
| **Rayon** | Parallel CPU computation |
| **Protocol Buffers** | Structured gRPC communication |
| **gRPC** | High-throughput streaming ingestion |
| **HTTP** | Binary telemetry ingestion |
| **UDP** | Low-overhead datagram ingestion |
| **Prometheus Format** | Operational metrics |
| **W3C Trace Context** | Distributed trace propagation |

---

## Design Philosophy

`fluerd` is built around five engineering principles:

1. **Zero-Copy First:** Avoid unnecessary memory allocation and copying during packet inspection.
2. **Explicit Ownership:** Maintain clear memory ownership across the Go-Rust FFI boundary.
3. **Bounded Concurrency:** Use bounded queues and non-blocking ingestion to prevent uncontrolled memory growth.
4. **Deterministic Batching:** Combine batch-size thresholds with timeout-based flushing.
5. **Parallel by Design:** Separate asynchronous I/O from CPU-intensive processing using Tokio and Rayon.

The result is a modular telemetry engine designed for high-throughput ingestion, predictable resource usage, and efficient parallel processing.

---

## Project Status

`fluerd` is an engineering project exploring high-throughput telemetry ingestion, Go-Rust interoperability, zero-copy parsing, asynchronous backpressure, and hybrid parallel processing.

The benchmark figures documented above represent the recorded results of the current implementation and its test environment.
