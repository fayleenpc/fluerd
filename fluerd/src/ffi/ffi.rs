use crate::parser::{OwnedTelemetryPacket, parse_raw_bytes};
use crate::pipeline::run_batch_consumer;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::slice;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Duration;

/// Opaque handle yang dipegang oleh Go sebagai `*C.EngineHandle`
pub struct EngineHandle {
    runtime: Runtime,
    tx: Option<mpsc::Sender<OwnedTelemetryPacket>>,
    consumer_task: Option<JoinHandle<Result<u64, tokio::task::JoinError>>>,
}

/// 1. Inisialisasi Tokio Runtime & Consumer Pipeline di sisi Rust
#[unsafe(no_mangle)]
pub extern "C" fn fluerd_init(
    channel_capacity: usize,
    batch_size: usize,
    flush_interval_ms: u64,
) -> *mut EngineHandle {
    let result = catch_unwind(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .ok()?;

        let (tx, rx) = mpsc::channel(channel_capacity);
        let interval = Duration::from_millis(flush_interval_ms);

        let consumer_task =
            runtime.spawn(async move { run_batch_consumer(rx, batch_size, interval).await });

        Some(Box::into_raw(Box::new(EngineHandle {
            runtime,
            tx: Some(tx),
            consumer_task: Some(consumer_task),
        })))
    });

    result.unwrap_or(None).unwrap_or(std::ptr::null_mut())
}

/// 2. Terima raw bytes dari Go, filter secara zero-copy, lalu kirim ke channel
/// Return: 0 = sukses masuk queue, 1 = non-kritis (diabaikan), -1 = frame rusak, -2 = queue penuh/error
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fluerd_push_frame(
    handle: *mut EngineHandle,
    data_ptr: *const u8,
    data_len: usize,
) -> i32 {
    if handle.is_null() || data_ptr.is_null() || data_len == 0 {
        return -1;
    }

    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: Caller (Go) menjamin `handle` valid dan `data_ptr` valid sepanjang `data_len`
        // selama pemanggilan sinkron ini berlangsung.
        let engine = unsafe { &*handle };
        let raw_slice = unsafe { slice::from_raw_parts(data_ptr, data_len) };

        let Ok(pkt) = parse_raw_bytes(raw_slice) else {
            return -1;
        };

        if !pkt.is_critical() {
            return 1;
        }

        // Konversi ke OwnedTelemetryPacket SEBELUM keluar dari batas FFI
        // karena Go GC bebas memindahkan memori `data_ptr` setelah fungsi ini return.
        let owned: OwnedTelemetryPacket = pkt.into();

        let Some(tx) = &engine.tx else {
            return -2;
        };

        // Menggunakan `try_send` agar FFI call dari goroutine tidak memblokir OS thread Go
        match tx.try_send(owned) {
            Ok(()) => 0,
            Err(_) => -2,
        }
    }));

    result.unwrap_or(-2)
}

/// 3. Tutup channel, flush sisa batch, ambil skor akhir, dan bebaskan memori handle
#[unsafe(no_mangle)]
pub unsafe extern "C" fn fluerd_shutdown(handle: *mut EngineHandle) -> u64 {
    if handle.is_null() {
        return 0;
    }

    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: Mengambil kembali kepemilikan `Box` untuk di-drop di akhir fungsi
        let mut engine = unsafe { Box::from_raw(handle) };

        // Drop sender agar consumer di Tokio tahu tidak ada data baru lagi
        drop(engine.tx.take());

        if let Some(task) = engine.consumer_task.take() {
            engine.runtime.block_on(task).unwrap_or(Ok(0)).unwrap_or(0)
        } else {
            0
        }
    }));

    result.unwrap_or(0)
}
