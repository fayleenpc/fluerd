package transport

import "github.com/fayleenpc/fluerd/pkg/engine"

// FrameIngestor adalah kontrak minimal yang dibutuhkan oleh semua layer transport.
type FrameIngestor interface {
	PushFrame(frame []byte) engine.PushStatus
}
