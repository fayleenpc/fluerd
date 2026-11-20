package engine_test

import (
	"testing"

	"github.com/fayleenpc/fluerd/pkg/engine"
	"github.com/fayleenpc/fluerd/pkg/protocol"
)

func TestEngineLifecycleAndScoring(t *testing.T) {
	eng, err := engine.New(engine.Config{
		ChannelCapacity: 16,
		BatchSize:       4,
		FlushIntervalMs: 50,
	})
	if err != nil {
		t.Fatalf("gagal inisialisasi engine: %v", err)
	}

	buf := make([]byte, 64)

	// 1. Kirim paket kritis (severity = 8, payload =  -> sum = 60, skor = 60 * 8 = 480)
	critFrame := protocol.TelemetryFrame{
		Severity: 8,
		DeviceID: []byte("test-gw-1"),
		Payload:  []byte{10, 20, 30},
	}
	n, err := critFrame.EncodeInto(buf)
	if err != nil {
		t.Fatalf("gagal encode frame kritis: %v", err)
	}
	if status := eng.PushFrame(buf[:n]); status != engine.StatusAccepted {
		t.Errorf("ekspektasi StatusAccepted (0), dapat %d", status)
	}

	// 2. Kirim paket non-kritis (severity = 3 -> harus diabaikan zero-copy oleh Rust)
	normFrame := protocol.TelemetryFrame{
		Severity: 3,
		DeviceID: []byte("test-gw-2"),
		Payload:  []byte{50, 50},
	}
	n, err = normFrame.EncodeInto(buf)
	if err != nil {
		t.Fatalf("gagal encode frame normal: %v", err)
	}
	if status := eng.PushFrame(buf[:n]); status != engine.StatusIgnored {
		t.Errorf("ekspektasi StatusIgnored (1), dapat %d", status)
	}

	// 3. Kirim frame korup (panjang header tidak sesuai)
	corrupt := []byte{9, 0, 50, 'b', 'a', 'd'}
	if status := eng.PushFrame(corrupt); status != engine.StatusCorrupt {
		t.Errorf("ekspektasi StatusCorrupt (-1), dapat %d", status)
	}

	// 4. Shutdown & verifikasi skor akhir dari Rust (harus 480)
	score := eng.Shutdown()
	if score != 480 {
		t.Errorf("ekspektasi total skor 480, dapat %d", score)
	}

	// 5. Pastikan double-shutdown aman (idempotent, tidak double-free pointer)
	if secondScore := eng.Shutdown(); secondScore != 0 {
		t.Errorf("ekspektasi 0 pada pemanggilan Shutdown kedua, dapat %d", secondScore)
	}
}
