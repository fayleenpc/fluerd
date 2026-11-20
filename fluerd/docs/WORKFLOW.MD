# WORKFLOW.MD — Peta Alur Data, Memori, & Eksekusi (Project Patroli)

## 1. Diagram Alir Data & Memori (End-to-End Dual Pipeline)

```text
[Network / Raw Bytes: Vec<u8>]  <-- (1x Alokasi di Ingestion Task)
              │
              ▼
┌──────────────────────────────────────────────────────────┐
│ PIPELINE 1: Zero-Copy Parser (`src/parser/mod.rs`)       │
│ - Gunakan `.first()` & `.get()` (100% Panic-Free)        │
│ - Bentuk `ZeroCopyPacket<'a>` (&str, &[u8])              │
│ - ALOKASI HEAP = 0 BYTES                                 │
└─────────────┬────────────────────────────────────────────┘
              │
       ┌──────┴───────┐
       ▼              ▼
 [Data Rusak /    [Paket Kritis]
  Non-Kritis]     (severity >= 7)
       │              │
       ▼              ▼
  (DROPPED)     `to_owned_packet()`  <-- Alokasi Heap HANYA untuk data lolos!
  0 Alloc             │
                      ▼
        ┌───────────────────────────┐
        │ Bounded Channel (Cap: 100)│ <-- Backpressure alami (Anti-OOM)
        │ `tokio::sync::mpsc`       │
        └─────────────┬─────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────────┐
│ PIPELINE 2: Smart Batching (`src/pipeline/mod.rs`)       │
│ - `tokio::select!` (Batch penuh = 15 ATAU Timeout 250ms) │
│ - `std::mem::replace` -> Tukar pointer Vec dalam O(1)    │
└─────────────────────┬────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────────┐
│ CPU Isolation & Parallel Compute                         │
│ - `tokio::task::spawn_blocking` (Jaga event-loop Tokio)  │
│ - `rayon::par_iter()` (Work-stealing semua core CPU)     │
│ - `AtomicU64::fetch_add` (Update metrik Lock-Free)       │
└──────────────────────────────────────────────────────────┘
```

---

## 2. Alur Siklus Hidup Task & Graceful Shutdown

1. **Inisialisasi:** Buat channel `(tx, rx)` dan state bersama `Arc<AtomicU64>`.
2. **Spawn Consumer:** Lempar `rx` ke `tokio::spawn` (`run_batch_consumer`).
3. **Spawn Producers:** Clone `tx` (`tx.clone()`) untuk setiap worker pengirim.
4. **Tutup Pintu Utama:** Panggil `drop(tx)` asli di `main()` segera setelah loop spawn producer selesai.
5. **Sinkronisasi:**
   - Tunggu semua producer selesai (`handle.await`).
   - Saat semua `tx_clone` habis ter-drop, `rx.recv()` di consumer otomatis menerima `None`.
   - Consumer mem-flush sisa buffer terakhir, lalu keluar dari loop dengan bersih.

---

## 3. Workflow 5 Langkah Saat Live Coding (Selasa, 24 Nov 2026)

1. **Pahami Kontrak Data & Error Dulu:**
   - Definisikan struct input/output dan enum `Error` (`thiserror`) sebelum menulis logika konkurensi.
2. **Tulis Fungsi Murni (Synchronous) & Unit Test Kecil:**
   - Pisahkan logika parsing/kalkulasi di modul terpisah agar mudah dites dengan `cargo test`.
3. **Bangun Pipa Async (`mpsc` + `tokio::select!`):**
   - Pasang kapasitas channel (*bounded*) sejak awal sebagai pelindung memori.
4. **Isolasi Beban Berat:**
   - Jika ada komputasi CPU berat di tengah alur async, langsung bungkus dengan `spawn_blocking`.
5. **Verifikasi Akhir Sebelum Selesai:**
   - Jalankan `cargo test && cargo clippy && cargo fmt` untuk memastikan kode bersih, idiomatis, dan bebas warning.
