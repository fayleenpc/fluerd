package telemetry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type traceCtxKey struct{}

// SpanRecord adalah representasi span yang selesai dieksekusi (kompatibel dengan model Jaeger).
type SpanRecord struct {
	TraceID    string            `json:"traceID"`
	SpanID     string            `json:"spanID"`
	ParentID   string            `json:"parentSpanID,omitempty"`
	Operation  string            `json:"operationName"`
	StartTime  int64             `json:"startTimeUs"`
	DurationUs int64             `json:"durationUs"`
	Tags       map[string]string `json:"tags"`
}

// TraceRegistry menyimpan N span terakhir di memori (ring buffer) untuk inspeksi cepat seperti Jaeger UI.
type TraceRegistry struct {
	mu       sync.RWMutex
	records  []SpanRecord
	capacity int
}

func NewTraceRegistry(capacity int) *TraceRegistry {
	if capacity <= 0 {
		capacity = 100
	}
	return &TraceRegistry{
		records:  make([]SpanRecord, 0, capacity),
		capacity: capacity,
	}
}

func (r *TraceRegistry) Record(rec SpanRecord) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.records) >= r.capacity {
		// Geser slice untuk membuang span paling lama (FIFO ring behavior)
		copy(r.records, r.records[1:])
		r.records[len(r.records)-1] = rec
		return
	}
	r.records = append(r.records, rec)
}

// TracesHandler mengekspos endpoint HTTP /traces (JSON / HTML ringan ala Jaeger).
func (r *TraceRegistry) TracesHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		r.mu.RLock()
		snapshot := make([]SpanRecord, len(r.records))
		copy(snapshot, r.records)
		r.mu.RUnlock()

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"service": "fluerd-patroli-collector",
			"count":   len(snapshot),
			"spans":   snapshot,
		})
	}
}

// Span merepresentasikan unit eksekusi aktif yang terikat pada context.Context.
type Span struct {
	TraceID   string
	SpanID    string
	ParentID  string
	Operation string
	StartTime time.Time
	Tags      map[string]string
	out       io.Writer
	registry  *TraceRegistry
}

func randomHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// StartSpan memulai span baru, mewarisi TraceID & ParentSpanID jika ada di context.
func StartSpan(ctx context.Context, reg *TraceRegistry, out io.Writer, operation string) (context.Context, *Span) {
	traceID := randomHex(16)
	var parentID string

	if parent, ok := ctx.Value(traceCtxKey{}).(*Span); ok && parent != nil {
		traceID = parent.TraceID
		parentID = parent.SpanID
	}

	span := &Span{
		TraceID:   traceID,
		SpanID:    randomHex(8),
		ParentID:  parentID,
		Operation: operation,
		StartTime: time.Now(),
		Tags:      make(map[string]string, 4),
		out:       out,
		registry:  reg,
	}

	return context.WithValue(ctx, traceCtxKey{}, span), span
}

func (s *Span) SetTag(k, v string) {
	s.Tags[k] = v
}

// Finish mencatat durasi span ke TraceRegistry dan output stream.
func (s *Span) Finish() {
	durUs := time.Since(s.StartTime).Microseconds()
	rec := SpanRecord{
		TraceID:    s.TraceID,
		SpanID:     s.SpanID,
		ParentID:   s.ParentID,
		Operation:  s.Operation,
		StartTime:  s.StartTime.UnixMicro(),
		DurationUs: durUs,
		Tags:       s.Tags,
	}

	if s.registry != nil {
		s.registry.Record(rec)
	}
	if s.out != nil {
		fmt.Fprintf(s.out, "[Trace] trace_id=%s span_id=%s parent=%s op=%s dur=%dµs tags=%v\n",
			rec.TraceID, rec.SpanID, rec.ParentID, rec.Operation, rec.DurationUs, rec.Tags)
	}
}
