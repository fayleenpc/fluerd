mod concepts;
mod parser;
mod pipeline;

use bytes::{BufMut, BytesMut};
use parser::{OwnedTelemetryPacket, parse_raw_bytes};
use pipeline::run_batch_consumer;
use std::fmt::Write;
use std::num::TryFromIntError;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{self, Duration};

struct PipelineConfig {
    channel_capacity: usize,
    batch_size: usize,
    flush_interval: Duration,
    worker_count: u8,
    messages_per_worker: u32,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 100,
            batch_size: 15,
            flush_interval: Duration::from_millis(250),
            worker_count: 3,
            messages_per_worker: 20,
        }
    }
}

fn encode_frame_into(
    buf: &mut BytesMut,
    severity: u8,
    device_id: &str,
    payload: &[u8],
) -> Result<(), TryFromIntError> {
    let id_len = u16::try_from(device_id.len())?;
    buf.clear();
    buf.put_u8(severity);
    buf.put_u16(id_len);
    buf.put_slice(device_id.as_bytes());
    buf.put_slice(payload);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = PipelineConfig::default();
    let (tx, rx) = mpsc::channel::<OwnedTelemetryPacket>(cfg.channel_capacity);

    // 1. Consumer mengembalikan `Result<u64, JoinError>` langsung (tanpa `Arc<AtomicU64>`)
    let consumer_handle =
        tokio::spawn(
            async move { run_batch_consumer(rx, cfg.batch_size, cfg.flush_interval).await },
        );

    // 2. Ingestion Workers dengan `JoinSet`
    let mut workers = JoinSet::new();
    for node_id in 1..=cfg.worker_count {
        let tx = tx.clone();
        let total_msgs = cfg.messages_per_worker;

        workers.spawn(async move {
            run_ingestion_worker(node_id, total_msgs, tx).await;
        });
    }

    drop(tx);

    while let Some(res) = workers.join_next().await {
        res?;
    }

    // Ambil total skor langsung dari return value consumer task
    let total_anomaly_score = consumer_handle.await??;

    println!("\n=== DUAL PIPELINE SELESAI === Total Skor Anomali Global: {total_anomaly_score}");

    concepts::demo_mutex_comparison().await;
    Ok(())
}

async fn run_ingestion_worker(
    node_id: u8,
    total_msgs: u32,
    tx: mpsc::Sender<OwnedTelemetryPacket>,
) {
    let mut frame_buf = BytesMut::with_capacity(64);
    let mut device_id_buf = String::with_capacity(32);

    for seq in 1..=total_msgs {
        if seq % 7 == 0 {
            frame_buf.clear();
            frame_buf.put_slice(&[9, 0, 50, b'b', b'a', b'd']);
        } else {
            let severity = if seq % 2 == 0 { 8 } else { 3 };
            device_id_buf.clear();
            let _ = write!(&mut device_id_buf, "node-{node_id}-sensor-{seq}");

            if let Err(e) = encode_frame_into(
                &mut frame_buf,
                severity,
                &device_id_buf,
                &[10, 20, 30, 40, 50],
            ) {
                eprintln!("Failed to encode frame: {e:?}");
                continue;
            }
        }

        let Ok(zero_copy_pkt) = parse_raw_bytes(&frame_buf) else {
            eprintln!("[Pipeline 1 - Drop] Frame rusak dari node-{node_id}");
            time::sleep(Duration::from_millis(10)).await;
            continue;
        };

        // Menggunakan `.into()` dari implementasi `From<ZeroCopyPacket<'_>>`
        if zero_copy_pkt.is_critical() && tx.send(zero_copy_pkt.into()).await.is_err() {
            break;
        }

        time::sleep(Duration::from_millis(10)).await;
    }
}
