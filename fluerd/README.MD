# fluerd — High-Throughput Telemetry & Systems Concurrency Reference

`fluerd` adalah purwarupa (*prototype*) sistem pemrosesan telemetri biner berkinerja tinggi yang dibangun menggunakan **Rust**, **Tokio**, dan **Rayon**. Proyek ini dirancang sebagai referensi arsitektur *systems programming* dengan fokus pada efisiensi memori (*zero-copy parsing*), manajemen *backpressure*, konkurensi asinkron, dan komputasi paralel *CPU-bound*.

---

## Arsitektur Sistem

Sistem ini memisahkan tahap **I/O Ingestion** (konkuren, *zero-copy filtering*) dari tahap **Batch Aggregation & Compute** (paralel, *hybrid size-or-timeout flushing*) menggunakan *bounded channel* sebagai mekanisme kontrol aliran (*backpressure*).

```text
+--------------------+     +--------------------+     +--------------------+

| Ingestion Worker 1 |     | Ingestion Worker 2 |     | Ingestion Worker 3 |
| (BytesMut Reuse)   |     | (BytesMut Reuse)   |     | (BytesMut Reuse)   |
+---------+----------+     +---------+----------+     +---------+----------+

          |                          |                          |
          |  1. Zero-Copy Parse (&[u8], &'a str)                |
          |  2. Filter non-kritis & frame rusak (0 heap alloc)  |
          +--------------------------+--------------------------+
                                     |
                                     v
                  [ Bounded mpsc::channel(capacity = 100) ]
                                     |
                                     v  (rx.recv_many)
                    +--------------------------------+

                    |    Batch Consumer (Tokio)      |
                    |  Flush jika:                   |
                    |  - Batch == 15 item, ATAU      |
                    |  - Timeout == 250ms (ticker)   |
                    +----------------+---------------+
                                     |
                                     v  (tokio::task::spawn_blocking)
                    +--------------------------------+

                    |   CPU Compute Pool (Rayon)     |
                    |   par_iter() -> saturating_mul |
                    +----------------+---------------+
                                     |
                                     v
                        Total Skor Anomali Global
```

---

## Struktur Modul & Pemetaan Konsep Inti

| Modul | Tanggung Jawab Utama | Konsep Rust yang Diterapkan |
| :--- | :--- | :--- |
| `src/main.rs` | Orkestrasi *ingestion workers*, manajemen siklus hidup *channel*, dan agregasi hasil akhir. | `JoinSet` (*structured concurrency*), buffer reuse (`BytesMut`, `String`), penutupan channel deterministik (`drop(tx)`). |
| `src/parser/mod.rs` | Dekode frame biner mentah menjadi struktur data bertipe aman tanpa alokasi memori awal. | *Explicit lifetimes* (`'a`), *slice pattern matching*, `split_at_checked`, `thiserror`, trait `From`. |
| `src/pipeline/mod.rs` | Konsumsi batch asinkron dengan batas ukuran/waktu serta eksekusi kalkulasi skor secara paralel. | `tokio::select!`, `rx.recv_many`, `MissedTickBehavior`, `std::mem::replace`, `spawn_blocking` + `rayon::par_iter`. |
| `src/concepts/mod.rs` | Perbandingan pola sinkronisasi *state* bersama di lingkungan multi-thread dan asinkron. | `std::sync::Mutex` (dengan *poison recovery*), `tokio::sync::Mutex` (lintas `.await`), dan *Actor Pattern*. |

---

## Bedah Teknis & Keputusan Desain

### 1. Memory Management & Zero-Copy Parsing (`src/parser/mod.rs`)
* **Protokol Frame Biner:** `[1B severity][2B id_len (Big-Endian)][NB device_id (UTF-8)][sisa payload]`
* **Zero-Copy Filtering:** Fungsi `parse_raw_bytes(&[u8]) -> Result<ZeroCopyPacket<'_>, ParseError>` meminjam (*borrow*) langsung dari buffer milik worker tanpa menyalin data ke heap. Paket dengan `severity < 7` atau format rusak langsung dibuang sebelum konversi ke `OwnedTelemetryPacket` dilakukan.
* **Anti-Panic Slicing:** Pemotongan slice menggunakan `.split_at_checked(id_len)` alih-alih pengindeksan langsung (`&rest[..id_len]`), sehingga input jaringan yang terpotong (*truncated*) selalu menghasilkan `Err(ParseError::PacketTooShort)` dan tidak pernah memicu *panic*.
* **Buffer Reuse:** Setiap *ingestion worker* mengalokasikan `BytesMut::with_capacity(64)` dan `String::with_capacity(32)` satu kali di luar loop, lalu memanggil `.clear()` di tiap iterasi untuk mencegah fragmentasi dan alokasi heap berulang.

### 2. Bounded Channel & Backpressure (`src/main.rs`)
* **Kapasitas Terbatas:** Menggunakan `tokio::sync::mpsc::channel(100)` untuk membatasi penggunaan memori jika kecepatan *producer* melampaui *consumer*.
* **Graceful Shutdown:** Seluruh *sender* milik worker di-drop otomatis saat `JoinSet` selesai, dan *sender* utama di-drop secara eksplisit (`drop(tx)`) sebelum menunggu `consumer_handle.await`. Hal ini menjamin *consumer* menerima sinyal `count == 0`, mem-flush sisa item di buffer, dan keluar tanpa *deadlock*.

### 3. Deterministik Hybrid Batching (`src/pipeline/mod.rs`)
* **Batch Retrieval (`rx.recv_many`):** Mengambil beberapa item sekaligus dari antrean channel dalam satu kali penguncian internal, mengurangi *lock contention* dibanding memanggil `rx.recv().await` berulang kali.
* **Size OR Timeout (`tokio::select!`):**
  * **Trigger Ukuran:** Langsung memicu `flush_batch` begitu `batch_buffer.len() >= max_batch_size`.
  * **Trigger Waktu:** Menggunakan `tokio::time::interval` dengan `MissedTickBehavior::Delay` dan guard `if !batch_buffer.is_empty()`. Timer di-reset (`ticker.reset()`) setiap kali buffer bertransisi dari kosong menjadi terisi (`was_empty`) agar setiap batch mendapat jatah waktu tunggu penuh sejak item pertamanya tiba.
* **Zero-Allocation Buffer Swap:** Menggunakan `std::mem::replace(batch_buffer, Vec::with_capacity(max_batch_size))` untuk memindahkan kepemilikan batch ke thread komputasi tanpa menyalin elemen.

### 4. Pemisahan Concurrency (Tokio) vs Parallelism (Rayon)
* **Anti-Blocking Runtime:** Kalkulasi skor paket (`hitung_skor_paket`) adalah tugas *CPU-bound*. Eksekusinya dipindahkan dari *event loop* Tokio menggunakan `tokio::task::spawn_blocking`, lalu diparalelkan di seluruh core CPU menggunakan `rayon::par_iter()`.
* **Aritmetika Aman:** Menggunakan `saturating_mul` pada perhitungan skor untuk mencegah *integer overflow* atau *wrap-around* yang dapat memanipulasi skor anomali.

### 5. Sinkronisasi State Konkuren (`src/concepts/mod.rs`)
Modul ini mendemonstrasikan tiga pendekatan pengelolaan *shared state* beserta batasan masing-masing:
1. **`std::sync::Mutex`:** Digunakan untuk *critical section* singkat yang murni sinkron (tanpa `.await` saat guard dipegang), dilengkapi pemulihan *poisoned lock* (`PoisonError::into_inner`).
2. **`tokio::sync::Mutex`:** Digunakan ketika *lock* harus dipertahankan melintasi titik suspensi `.await` (misalnya saat melakukan operasi I/O berurutan).
3. **Actor Pattern (`mpsc` + `oneshot`):** Menghilangkan kebutuhan *mutex* sepenuhnya dengan mengisolasi *state* di dalam satu *task* pemilik tunggal dan berkomunikasi melalui pengiriman pesan.

---

## Cara Menjalankan & Validasi Kualitas

### Prasyarat
* Rust toolchain terbaru (disarankan Rust 1.80+ dengan dukungan `split_at_checked`)
* Cargo

### Perintah Eksekusi
```bash
# 1. Jalankan simulasi dual-pipeline dan demo sinkronisasi
cargo run

# 2. Jalankan unit test (parser zero-copy & penanganan error)
cargo test

# 3. Audit kualitas kode dengan aturan ketat Clippy Pedantic (Target: 0 warnings)
cargo clippy -- -W clippy::pedantic

# 4. Bangun dan buka dokumentasi lokal dari doc-comments
cargo doc --no-deps --open
```

---

## Catatan Pengembangan Lanjutan (Production Readiness)
* **True Zero-Copy End-to-End (`bytes::Bytes`):** Mengganti `String` dan `Vec<u8>` pada `OwnedTelemetryPacket` dengan `bytes::Bytes` agar pemotongan payload (`Bytes::slice`) lintas channel bersifat $O(1)$ tanpa alokasi ulang di heap.
* **Dedicated Rayon Bridge:** Pada beban CPU ekstrem, mengganti `tokio::task::spawn_blocking` dengan `rayon::spawn` yang dipasangkan dengan `tokio::sync::oneshot::channel` agar *blocking thread pool* milik Tokio tetap dikhususkan untuk operasi I/O sinkron.
* **Structured Cancellation (`CancellationToken`):** Menambahkan `tokio_util::sync::CancellationToken` untuk menangani sinyal terminasi OS (`SIGINT`/`SIGTERM`) secara terstruktur.
