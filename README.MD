# fluerd — High-Throughput Telemetry Engine & Multi-Protocol Go-Rust Orchestrator

`fluerd` adalah sistem pemrosesan telemetri biner berkinerja tinggi berbasis arsitektur hibrida **Go + Rust (`cdylib` FFI)**. Sistem ini memisahkan lapisan **Multi-Protocol Ingestion & Observability** di sisi Go (gRPC/Protobuf, HTTP, UDP, Prometheus, & Jaeger-style UI) dari **Zero-Copy Data-Plane & Parallel Compute Engine** di sisi Rust (Tokio & Rayon) tanpa *Garbage Collection overhead*.

---

## Arsitektur End-to-End

```text
+-----------------------------------------------------------------------------+

|                        GO ORCHESTRATOR (orchestrator/)                      |
|                                                                             |
|  [gRPC :50051]       [HTTP :8081]        [UDP :9000]       [CLI Bench/Sim]  |
|   (Protobuf)         (POST /ingest)      (sync.Pool)         (1M+ Frames)   |
|       \                    |                  /                   /         |
|        +-------------------+-----------------+-------------------+          |
|                            |                                                |
|                            v  (Zero-Alloc Binary Wire Format)               |
|               [ pkg/engine — Safe cgo FFI Wrapper ]                         |
|           * Lock-free atomic metrics & W3C Trace Spans                      |
|           * UI Console & Prometheus (/metrics, /traces) di :9090            |
+----------------------------+------------------------------------------------+
                             |
                             |  extern "C" FFI Boundary (~273 ns / call)
                             |  (catch_unwind panic guard + try_send)
                             v
+-----------------------------------------------------------------------------+

|                         RUST CORE ENGINE (fluerd/)                          |
|                                                                             |
|  1. Zero-Copy Gatekeeper (src/parser/mod.rs)                                |
|     * Slice pattern matching & split_at_checked (&[u8], &'a str)            |
|     * Drop frame rusak & non-kritis (severity < 7) dengan 0 heap allocation |
|                            |                                                |
|                            v                                                |
|  2. Bounded Channel Backpressure (tokio::sync::mpsc::channel)               |
|     * Menolak lonjakan berlebih (StatusBackpressure) agar RAM tetap stabil  |
|                            |                                                |
|                            v  (rx.recv_many)                                |
|  3. Hybrid Batch Consumer (src/pipeline/mod.rs)                             |
|     * tokio::select! -> Flush jika Batch penuh ATAU Timeout tercapai        |
|     * O(1) buffer pointer swap via std::mem::replace                        |
|                            |                                                |
|                            v  (tokio::task::spawn_blocking)                 |
|  4. Parallel CPU Compute Pool (Rayon)                                       |
|     * batch.par_iter() dengan saturating_mul anti-overflow                  |
+-----------------------------------------------------------------------------+
```

---

## Struktur Direktori Monorepo

```text
fluerd/
├── fluerd/                                # Rust Core Engine (cdylib + rlib + standalone bin)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                         # Library entrypoint untuk shared library (libfluerd.so)
│       ├── main.rs                        # Standalone Rust pipeline & concurrency runner
│       ├── ffi/ffi.rs                     # Safe extern "C" FFI boundary & panic guard
│       ├── parser/mod.rs                  # Zero-copy binary parser dengan lifetime ('a)
│       ├── pipeline/mod.rs                # Hybrid size-or-timeout batching & Rayon compute
│       └── concepts/mod.rs                # Perbandingan StdMutex, TokioMutex, & Actor pattern
│
└── orchestrator/                          # Go Multi-Protocol Gateway & Observability
    ├── Makefile                           # Otomatisasi proto-gen, Rust release build, cgo, & test
    ├── go.mod
    ├── cmd/
    │   ├── collector/main.go              # Daemon multi-protokol (gRPC, HTTP, UDP, UI :9090)
    │   ├── bench/main.go                  # Stress-test in-memory FFI (1.000.000 frame)
    │   └── simulator/main.go              # Generator trafik jaringan (gRPC stream, UDP, Wave)
    ├── internal/
    │   ├── shared/telemetry/              # Observabilitas 100% Native Go (zero external deps)
    │   │   ├── metrics.go                 # Prometheus text exposition (/metrics)
    │   │   ├── tracer.go                  # Jaeger-style ring-buffer span collector (/traces)
    │   │   └── dashboard.go               # Interactive Web UI Console (GET /)
    │   └── transport/                     # Layer penerima jaringan terisolasi
    │       ├── transport.go               # Kontrak interface FrameIngestor
    │       ├── grpc_server.go             # gRPC Unary & Client-Streaming server (:50051)
    │       ├── http_server.go             # HTTP POST /ingest dengan CORS & MaxBytesReader (:8081)
    │       └── udp_server.go              # High-frequency UDP datagram listener (:9000)
    └── pkg/
        ├── common/
        │   ├── proto/patroli.proto        # Kontrak Protobuf v3
        │   └── configpb/                  # Hasil generate protoc (Go & gRPC)
        ├── engine/
        │   ├── engine.go                  # Wrapper Go aman pembungkus unsafe cgo pointer
        │   └── engine_test.go             # Tes integrasi siklus hidup FFI & idempotent shutdown
        └── protocol/
            └── frame.go                   # Encoder wire-format biner zero-allocation
```

---

## Hasil Benchmark & Karakteristik Performa

Pengujian dilakukan pada mesin Linux x86_64 dengan konfigurasi *bounded channel* `10.000`, `BatchSize = 512`, dan `FlushInterval = 100ms`:

| Skenario Pengujian | Beban Trafik | Throughput Efektif | Latensi / Karakteristik Kunci |
| :--- | :--- | :--- | :--- |
| **In-Memory FFI Burst (`make run-bench`)** | 1.000.000 frame (100 goroutine) | **~16,29 juta frame/detik** | Rata-rata FFI call **0,273 µs (273 ns)**; 800k paket normal gugur *zero-copy*; 161k paket berlebih ditahan *backpressure*. |
| **gRPC Client-Streaming (`make sim-grpc`)** | 100.000 pesan Protobuf (20 stream) | **~2,74 juta frame/detik** | Total waktu **36,4 ms** melalui HTTP/2 lokal (`:50051`). |
| **UDP Datagram Flood (`make sim-udp`)** | 200.000 paket biner (50 worker) | **~1,28 juta frame/detik** | Total waktu **155,7 ms** dengan *zero-alloc* `sync.Pool` (`:9000`). |
| **Wave Traffic (`make sim-wave`)** | 5 gelombang × 400 paket (jeda 200ms) | **Deterministik Timeout Flush** | Memvalidasi `tokio::select!` mem-flush batch parsial (< 512) saat trafik sepi. |
| **Jejak Memori Runtime (`collector`)** | 4 server aktif + gRPC + UI | **< 3 MB Go Heap** | Konsumsi memori stabil tanpa lonjakan GC saat menerima *burst* trafik. |

### Catatan Trade-Off Rekayasa (*Latency vs Throughput*)
* **Sub-Mikrodetik di Gerbang Masuk:** Validasi protokol dan pembuangan paket non-kritis (`severity < 7`) terjadi secara sinkron di dalam *zero-copy parser* Rust dalam hitungan ratusan nanodetik tanpa alokasi heap.
* **Batching untuk Throughput Maksimal:** Tahap agregasi sengaja menahan paket hingga `512` item atau maksimal `100ms` agar eksekusi paralel `rayon::par_iter()` bekerja optimal dengan biaya *context switch* minimal saat menghadapi lonjakan trafik masif.

---

## Bedah Teknis & Keputusan Desain

### 1. Kontrak Protokol Biner (`pkg/protocol` & `src/parser/mod.rs`)
```text
+---------------+-------------------+-----------------------+--------------------+

| Severity (1B) | ID Length (2B BE) | Device ID (N Bytes)   | Payload (Sisa)     |
+---------------+-------------------+-----------------------+--------------------+
```
* **Zero-Copy Filtering:** `parse_raw_bytes(&[u8]) -> Result<ZeroCopyPacket<'_>, ParseError>` meminjam langsung *slice* dari memori pemanggil. Hanya paket kritis (`severity >= 7`) yang dikonversi menjadi `OwnedTelemetryPacket` sebelum dikirim melintasi batas FFI atau *channel*.
* **Anti-Panic Slicing:** Menggunakan *slice pattern matching* dan `.split_at_checked(id_len)` sehingga frame terpotong (*truncated*) selalu mengembalikan `ParseError::PacketTooShort` tanpa memicu *panic*.

### 2. Keamanan Batas `cgo` FFI (`src/ffi/ffi.rs` & `pkg/engine/engine.go`)
* **Non-Blocking FFI (`try_send`):** Karena pemanggilan `cgo` mengunci satu OS thread (`M`) milik Go runtime, sisi Rust menggunakan `tx.try_send()` agar fungsi FFI tidak pernah memblokir dan langsung mengembalikan kode status *backpressure* (`-2`) jika antrean penuh.
* **Panic Boundary (`catch_unwind`):** Seluruh fungsi `extern "C"` dibungkus `std::panic::catch_unwind` untuk mencegah *stack unwinding* lintas bahasa yang dapat menyebabkan *Undefined Behavior* pada proses Go.
* **Memory Ownership Transfer:** Go menjamin *slice* input tetap hidup selama pemanggilan sinkron `fluerd_push_frame`, sementara Rust menyalin data paket kritis ke heap milik Rust sebelum fungsi FFI kembali.

### 3. Deterministik Hybrid Batching & Compute (`src/pipeline/mod.rs`)
* **Batch Channel Read (`rx.recv_many`):** Mengambil hingga `remaining_cap` item sekaligus dalam satu kali penguncian internal channel.
* **Resettable Timeout (`tokio::select!`):** Timer `tokio::time::interval` (dengan `MissedTickBehavior::Delay`) di-reset setiap kali `batch_buffer` bertransisi dari kosong menjadi terisi (`was_empty`), memastikan setiap batch mendapat jatah waktu tunggu penuh sejak paket pertamanya tiba.
