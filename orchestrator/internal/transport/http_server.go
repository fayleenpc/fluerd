package transport

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/fayleenpc/fluerd/internal/shared/telemetry"
	"github.com/fayleenpc/fluerd/pkg/engine"
)

type HTTPCollector struct {
	server   *http.Server
	ingestor FrameIngestor
	metrics  *telemetry.Metrics
	tracer   *telemetry.TraceRegistry
}

func NewHTTPCollector(
	addr string,
	ingestor FrameIngestor,
	metrics *telemetry.Metrics,
	tracer *telemetry.TraceRegistry,
) *HTTPCollector {
	h := &HTTPCollector{
		ingestor: ingestor,
		metrics:  metrics,
		tracer:   tracer,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", h.handleIngest)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	h.server = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return h
}

func (h *HTTPCollector) handleIngest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	_, span := telemetry.StartSpan(r.Context(), h.tracer, nil, "http.ingest")
	span.SetTag("protocol", "http")
	span.SetTag("remote_addr", r.RemoteAddr)
	defer span.Finish()

	w.Header().Set("X-Trace-ID", span.TraceID)

	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)
	defer r.Body.Close()

	raw, err := io.ReadAll(r.Body)
	if err != nil || len(raw) == 0 {
		span.SetTag("error", "invalid_payload")
		http.Error(w, "invalid frame payload", http.StatusBadRequest)
		return
	}

	span.SetTag("frame_bytes", fmt.Sprintf("%d", len(raw)))

	t0 := time.Now()
	status := h.ingestor.PushFrame(raw)
	if h.metrics != nil {
		h.metrics.ObservePush(int32(status), uint64(time.Since(t0).Nanoseconds()))
	}
	span.SetTag("ffi_status", fmt.Sprintf("%d", status))

	switch status {
	case engine.StatusAccepted, engine.StatusIgnored:
		w.WriteHeader(http.StatusAccepted)
	case engine.StatusBackpressure:
		http.Error(w, "engine queue full (backpressure)", http.StatusTooManyRequests)
	default:
		http.Error(w, "corrupt telemetry frame", http.StatusUnprocessableEntity)
	}
}

func (h *HTTPCollector) ListenAndServe(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = h.server.Shutdown(shutdownCtx)
	}()

	err := h.server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}
