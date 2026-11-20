use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{Mutex as TokioMutex, mpsc};
use tokio::time::{Duration, sleep};

/// Pola Idiomatik Rust: Enkapsulasi `Arc<StdMutex<T>>` di dalam struct ringan (`Clone`).
/// Pemanggil tidak perlu tahu detail locking di dalamnya (mirip method dengan receiver pointer di Go).
#[derive(Clone, Default)]
struct FastMetrics {
    count: Arc<StdMutex<u64>>,
}

impl FastMetrics {
    fn add(&self, val: u64) {
        // Scope lock dijaga tetap sangat sempit (hanya operasi CPU/memori)
        // let mut guard = self
        //     .count
        //     .lock()
        //     .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Cukup teruskan referensi method-nya langsung:
        // .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut guard = self
            .count
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        *guard += val;
    }

    fn get(&self) -> u64 {
        *self
            .count
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub async fn demo_mutex_comparison() {
    println!("\n=== DEMO KONSEP: std::sync::Mutex vs tokio::sync::Mutex vs Actor ===");

    // 1. std::sync::Mutex yang terenkapsulasi (untuk state memori singkat tanpa .await)
    let metrics = FastMetrics::default();
    let m1 = metrics.clone();
    let m2 = metrics.clone();

    let (r1, r2) = tokio::join!(
        tokio::spawn(async move { m1.add(50) }),
        tokio::spawn(async move { m2.add(50) }),
    );
    r1.unwrap();
    r2.unwrap();
    println!("[StdMutex] Update konkuren selesai: {}", metrics.get());

    // 2. tokio::sync::Mutex (aman ditahan melewati .await karena Guard-nya mengimplementasikan `Send`)
    let shared_io_buffer = Arc::new(TokioMutex::new(String::from("Header:")));
    let buf1 = Arc::clone(&shared_io_buffer);
    let buf2 = Arc::clone(&shared_io_buffer);

    let (t1, t2) = tokio::join!(
        tokio::spawn(async move {
            let mut guard = buf1.lock().await;
            guard.push_str("Data-1;");
            sleep(Duration::from_millis(10)).await; // Aman melewati .await
            guard.push_str("Ack-1;");
        }),
        tokio::spawn(async move {
            let mut guard = buf2.lock().await;
            guard.push_str("Data-2;");
            sleep(Duration::from_millis(10)).await;
            guard.push_str("Ack-2;");
        }),
    );
    t1.unwrap();
    t2.unwrap();
    println!(
        "[TokioMutex] Update lintas .await selesai: {}",
        *shared_io_buffer.lock().await
    );

    // 3. Pola Paling Idiomatik untuk Shared I/O: Actor / Dedicated Task (tanpa Mutex!)
    let (io_tx, mut io_rx) = mpsc::channel::<&'static str>(16);
    let io_actor = tokio::spawn(async move {
        let mut owned_buffer = String::from("ActorHeader:");
        while let Some(chunk) = io_rx.recv().await {
            owned_buffer.push_str(chunk);
            sleep(Duration::from_millis(5)).await; // Tidak ada lock sama sekali!
        }
        owned_buffer
    });

    io_tx.send("Event-A;").await.unwrap();
    io_tx.send("Event-B;").await.unwrap();
    drop(io_tx); // Tutup channel agar actor selesai

    let final_output = io_actor.await.unwrap();
    println!("[Actor Pattern] Hasil tanpa Mutex: {final_output}");
}
