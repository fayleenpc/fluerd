package engine

/*
#cgo LDFLAGS: -L../../../fluerd/target/release -lfluerd
#include <stdint.h>
#include <stddef.h>

typedef struct EngineHandle EngineHandle;

EngineHandle* fluerd_init(size_t capacity, size_t batch_size, uint64_t flush_ms);
int32_t fluerd_push_frame(EngineHandle* handle, const uint8_t* data_ptr, size_t data_len);
uint64_t fluerd_shutdown(EngineHandle* handle);
*/
import "C"
import (
	"errors"
	"sync/atomic"
	"unsafe"
)

// PushStatus merepresentasikan hasil pemrosesan frame dari Rust engine.
type PushStatus int32

const (
	StatusAccepted     PushStatus = 0  // Paket kritis berhasil masuk bounded channel
	StatusIgnored      PushStatus = 1  // Paket valid tapi non-kritis (di-drop zero-copy)
	StatusCorrupt      PushStatus = -1 // Frame rusak / gagal validasi protokol
	StatusBackpressure PushStatus = -2 // Channel penuh (backpressure aktif) atau engine tertutup
)

// Config mengatur kapasitas antrean dan perilaku batching di sisi Rust.
type Config struct {
	ChannelCapacity int
	BatchSize       int
	FlushIntervalMs uint64
}

// Engine membungkus pointer C ke Tokio runtime & pipeline Rust secara aman.
type Engine struct {
	handle *C.EngineHandle
	closed atomic.Bool
}

// New menginisialisasi Tokio runtime dan batch consumer di Rust.
func New(cfg Config) (*Engine, error) {
	if cfg.ChannelCapacity <= 0 || cfg.BatchSize <= 0 {
		return nil, errors.New("kapasitas channel dan batch size harus > 0")
	}

	h := C.fluerd_init(
		C.size_t(cfg.ChannelCapacity),
		C.size_t(cfg.BatchSize),
		C.uint64_t(cfg.FlushIntervalMs),
	)
	if h == nil {
		return nil, errors.New("gagal menginisialisasi Rust fluerd engine")
	}

	return &Engine{handle: h}, nil
}

// PushFrame mengirimkan slice byte mentah ke Rust tanpa alokasi di sisi Go.
func (e *Engine) PushFrame(frame []byte) PushStatus {
	if e.closed.Load() || len(frame) == 0 {
		return StatusCorrupt
	}

	status := C.fluerd_push_frame(
		e.handle,
		(*C.uint8_t)(unsafe.Pointer(&frame[0])),
		C.size_t(len(frame)),
	)
	return PushStatus(status)
}

// Shutdown menutup channel di Rust, mem-flush sisa batch, dan membebaskan memori.
func (e *Engine) Shutdown() uint64 {
	if !e.closed.CompareAndSwap(false, true) {
		return 0
	}
	score := C.fluerd_shutdown(e.handle)
	e.handle = nil
	return uint64(score)
}
