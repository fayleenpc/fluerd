cat << 'EOF' > HOW.md
# HOW.MD — Playbook Live Coding Rust (Project Patroli)

## 1. Perintah Wajib CLI (`cargo`)
- `cargo new <nama>` : Inisialisasi proyek baru.
- `cargo add tokio --features full rayon thiserror` : Pasang dependency utama via CLI.
- `cargo check` : Cek error compiler super cepat tanpa build binary.
- `cargo test` : Jalankan unit test (`#[cfg(test)]`).
- `cargo clippy` : Linter resmi untuk memastikan kode idiomatis & minim alokasi berlebih.
- `cargo fmt` : Rapikan format kode secara otomatis sebelum review selesai.

---

## 2. Pola Arsitektur "Golden Skeleton" (Dual-Pipeline)
1. **Zero-Copy Ingestion (`&[u8]` & `&str`):**
   - Validasi & filter data mentah di atas slice jaringan menggunakan lifetime `'a` (`ZeroCopyPacket<'a>`).
   - **Aturan:** Tolak data rusak/non-kritis **sebelum** konversi ke `String` atau `Vec<u8>` (0 alokasi heap).
2. **Backpressure Transport (`tokio::sync::mpsc`):**
   - Selalu gunakan **bounded channel** (misal kapasitas `100`) untuk mencegah RAM meledak (OOM) saat producer lebih cepat dari consumer.
   - `drop(tx)` asli di `main()` agar `rx.recv()` otomatis mendeteksi *graceful shutdown* (`None`).
3. **Smart Batching (`tokio::select!` + `std::mem::replace`):**
   - Gabungkan trigger ukuran batch (`batch.len() >= max`) dan timeout (`ticker.tick()`).
   - Gunakan `std::mem::replace` saat *flush* untuk menukar pointer `Vec` dalam waktu konstan **O(1)** tanpa copy isi data.
4. **CPU Isolation (`spawn_blocking` + Rayon):**
   - Bungkus komputasi paralel berat (`par_iter()`) dengan `tokio::task::spawn_blocking` agar worker thread async Tokio tidak terblokir.

---

## 3. Matriks Pemilihan Concurrency Primitives
- **`AtomicU64` / `AtomicUsize`:** Pilihan pertama untuk counter/metrik global (*lock-free*).
- **`std::sync::Mutex`:** Untuk mengunci struct/data di memori secara singkat (**dilarang ada `.await`** di dalam scope lock).
- **`tokio::sync::Mutex`:** Hanya dipakai jika lock harus ditahan melewati titik `.await` (misal menulis ke file/socket async).

---

## 4. Strategi Komunikasi Saat Live Coding (Selasa, 24 Nov 2026)
- **Pragmatis terhadap Borrow Checker:** Jika mentok di *lifetime* rumit saat waktu mepet, gunakan `.clone()` atau `Arc<T>` agar kode berjalan dulu, lalu sampaikan secara lisan: *"Di produksi, bagian ini akan kita ubah ke zero-copy slice untuk menghemat alokasi."*
- **Thinking Out Loud:** Selalu hubungkan keputusan kode dengan rekam jejak Anda (misal: *"Bounded channel ini menjaga RAM tetap stabil, sama seperti pendekatan saya menjaga kontainer di bawah 6,5 GB saat menarik data Arrow"*).
- **Anti-Panic:** Tangani error parsing/I/O dengan `Result<T, E>` dan operator `?`, hindari `.unwrap()` pada input eksternal.
EOF
