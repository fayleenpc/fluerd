use crate::parser::OwnedTelemetryPacket;
use rayon::prelude::*;
use tokio::sync::mpsc;
use tokio::task::JoinError;
use tokio::time::{self, Duration, MissedTickBehavior};

// fn hitung_skor_paket(packet: &OwnedTelemetryPacket) -> u64 {
//     let sum_bytes: u64 = packet.payload.iter().map(|&b| u64::from(b)).sum();
//     sum_bytes.wrapping_mul(u64::from(packet.severity))
// }

fn hitung_skor_paket(packet: &OwnedTelemetryPacket) -> u64 {
    let sum_bytes: u64 = packet.payload.iter().map(|&b| u64::from(b)).sum();
    sum_bytes.saturating_mul(u64::from(packet.severity))
}

fn proses_batch_paralel(batch: &[OwnedTelemetryPacket]) -> u64 {
    batch.par_iter().map(hitung_skor_paket).sum()
}

pub async fn run_batch_consumer(
    mut rx: mpsc::Receiver<OwnedTelemetryPacket>,
    max_batch_size: usize,
    flush_interval: Duration,
) -> Result<u64, JoinError> {
    let mut batch_buffer = Vec::with_capacity(max_batch_size);
    let mut total_anomaly_score: u64 = 0;

    let mut ticker = time::interval(flush_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        // Hitung sisa kapasitas di luar pemanggilan `recv_many` untuk menghindari konflik borrow
        let remaining_cap = max_batch_size - batch_buffer.len();
        let was_empty = batch_buffer.is_empty();

        tokio::select! {
            count = rx.recv_many(&mut batch_buffer, remaining_cap) => {
                if count == 0 {
                    if !batch_buffer.is_empty() {
                        total_anomaly_score += flush_batch(&mut batch_buffer, max_batch_size).await?;
                    }
                    println!("[Pipeline 2] Channel ditutup. Semua sisa batch selesai di-flush.");
                    break;
                }

                if batch_buffer.len() >= max_batch_size {
                    total_anomaly_score += flush_batch(&mut batch_buffer, max_batch_size).await?;
                } else if was_empty {
                    ticker.reset();
                }
            }

            _ = ticker.tick(), if !batch_buffer.is_empty() => {
                println!("[Pipeline 2] Flush dipicu timeout ({flush_interval:?})...");
                total_anomaly_score += flush_batch(&mut batch_buffer, max_batch_size).await?;
            }
        }
    }

    Ok(total_anomaly_score)
}

async fn flush_batch(
    batch_buffer: &mut Vec<OwnedTelemetryPacket>,
    max_batch_size: usize,
) -> Result<u64, JoinError> {
    let current_batch = std::mem::replace(batch_buffer, Vec::with_capacity(max_batch_size));
    let batch_len = current_batch.len();

    let sample_device = current_batch
        .first()
        .map_or_else(|| String::from("unknown"), |p| p.device_id.clone());

    let batch_score =
        tokio::task::spawn_blocking(move || proses_batch_paralel(&current_batch)).await?;

    println!(
        "-> [Flush Sukses] {batch_len} paket kritis | Skor batch: {batch_score} | Sampel awal: {sample_device}"
    );

    Ok(batch_score)
}
