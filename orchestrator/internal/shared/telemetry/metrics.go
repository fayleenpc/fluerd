package telemetry

import (
	"fmt"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"
)

// Metrics menyimpan seluruh counter performa lintas batas Go <-> Rust FFI secara lock-free.
type Metrics struct {
	FramesAccepted     atomic.Uint64
	FramesIgnored      atomic.Uint64
	FramesCorrupt      atomic.Uint64
	FramesBackpressure atomic.Uint64
	PushLatencyNsTotal atomic.Uint64
	PushCallsTotal     atomic.Uint64
	LastAnomalyScore   atomic.Uint64
	startTime          time.Time
}

func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

// ObservePush mencatat status dan durasi FFI call dalam nanodetik tanpa alokasi.
func (m *Metrics) ObservePush(status int32, durationNs uint64) {
	m.PushCallsTotal.Add(1)
	m.PushLatencyNsTotal.Add(durationNs)

	switch status {
	case 0:
		m.FramesAccepted.Add(1)
	case 1:
		m.FramesIgnored.Add(1)
	case -1:
		m.FramesCorrupt.Add(1)
	case -2:
		m.FramesBackpressure.Add(1)
	}
}

// PrometheusHandler menyajikan endpoint /metrics siap-scrape untuk Prometheus & Grafana.
func (m *Metrics) PrometheusHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		// 1. Metrik Pipeline & FFI
		fmt.Fprintf(w, "# HELP fluerd_frames_total Total telemetry frames processed by status.\n")
		fmt.Fprintf(w, "# TYPE fluerd_frames_total counter\n")
		fmt.Fprintf(w, "fluerd_frames_total{status=\"accepted\"} %d\n", m.FramesAccepted.Load())
		fmt.Fprintf(w, "fluerd_frames_total{status=\"ignored\"} %d\n", m.FramesIgnored.Load())
		fmt.Fprintf(w, "fluerd_frames_total{status=\"corrupt\"} %d\n", m.FramesCorrupt.Load())
		fmt.Fprintf(w, "fluerd_frames_total{status=\"backpressure\"} %d\n", m.FramesBackpressure.Load())

		// 2. Latensi FFI Call (Rata-rata dalam mikrodetik)
		calls := m.PushCallsTotal.Load()
		var avgLatencyUs float64
		if calls > 0 {
			avgLatencyUs = float64(m.PushLatencyNsTotal.Load()) / float64(calls) / 1000.0
		}
		fmt.Fprintf(w, "# HELP fluerd_ffi_push_avg_microseconds Average Go-to-Rust FFI call duration.\n")
		fmt.Fprintf(w, "# TYPE fluerd_ffi_push_avg_microseconds gauge\n")
		fmt.Fprintf(w, "fluerd_ffi_push_avg_microseconds %.3f\n", avgLatencyUs)

		// 3. Skor Anomali & Runtime Go Memory (penting untuk audit batas RAM)
		fmt.Fprintf(w, "# HELP fluerd_anomaly_score_total Latest aggregated anomaly score from Rust.\n")
		fmt.Fprintf(w, "# TYPE fluerd_anomaly_score_total gauge\n")
		fmt.Fprintf(w, "fluerd_anomaly_score_total %d\n", m.LastAnomalyScore.Load())

		fmt.Fprintf(w, "# HELP go_heap_alloc_bytes Current Go heap allocation in bytes.\n")
		fmt.Fprintf(w, "# TYPE go_heap_alloc_bytes gauge\n")
		fmt.Fprintf(w, "go_heap_alloc_bytes %d\n", mem.HeapAlloc)

		fmt.Fprintf(w, "# HELP go_goroutines_count Active goroutines.\n")
		fmt.Fprintf(w, "# TYPE go_goroutines_count gauge\n")
		fmt.Fprintf(w, "go_goroutines_count %d\n", runtime.NumGoroutine())
	}
}
