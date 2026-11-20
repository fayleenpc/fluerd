package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/fayleenpc/fluerd/internal/shared/telemetry"
	"github.com/fayleenpc/fluerd/pkg/engine"
	"github.com/fayleenpc/fluerd/pkg/protocol"
)

func runStressScenario(name string, workers, msgsPerWorker int, cfg engine.Config) {
	eng, err := engine.New(cfg)
	if err != nil {
		panic(err)
	}

	metrics := telemetry.NewMetrics()
	var wg sync.WaitGroup

	payload := []byte{10, 20, 30, 40, 50, 60, 70, 80}
	start := time.Now()

	for nodeID := 1; nodeID <= workers; nodeID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			frameBuf := make([]byte, 128)
			corruptFrame := []byte{9, 0, 50, 'b', 'a', 'd'}
			prefix := []byte("patroli-gw-" + strconv.Itoa(id) + "-if-")
			devIDBuf := make([]byte, 0, 48)

			for seq := 1; seq <= msgsPerWorker; seq++ {
				t0 := time.Now()
				var status engine.PushStatus

				if seq%100 == 0 {
					status = eng.PushFrame(corruptFrame)
				} else {
					sev := uint8(3)
					if seq%5 == 0 {
						sev = 8
					}
					devIDBuf = append(devIDBuf[:0], prefix...)
					devIDBuf = strconv.AppendInt(devIDBuf, int64(seq), 10)

					frame := protocol.TelemetryFrame{
						Severity: sev,
						DeviceID: devIDBuf,
						Payload:  payload,
					}
					n, err := frame.EncodeInto(frameBuf)
					if err != nil {
						metrics.ObservePush(int32(engine.StatusCorrupt), 0)
						continue
					}
					status = eng.PushFrame(frameBuf[:n])
				}

				metrics.ObservePush(int32(status), uint64(time.Since(t0).Nanoseconds()))
			}
		}(nodeID)
	}

	wg.Wait()
	ingestElapsed := time.Since(start)

	totalScore := eng.Shutdown()
	metrics.LastAnomalyScore.Store(totalScore)
	totalElapsed := time.Since(start)

	totalFrames := float64(workers * msgsPerWorker)
	calls := metrics.PushCallsTotal.Load()
	var avgFfiUs float64
	if calls > 0 {
		avgFfiUs = float64(metrics.PushLatencyNsTotal.Load()) / float64(calls) / 1000.0
	}

	fmt.Printf("\n=== SKENARIO: %s ===\n", name)
	fmt.Printf("Waktu Ingestion  : %v\n", ingestElapsed)
	fmt.Printf("Waktu Total      : %v (%.0f frame/detik)\n", totalElapsed, totalFrames/totalElapsed.Seconds())
	fmt.Printf("Rata-rata FFI    : %.3f µs/call\n", avgFfiUs)
	fmt.Printf("Diterima         : %d | Diabaikan: %d | Rusak: %d | Backpressure: %d\n",
		metrics.FramesAccepted.Load(),
		metrics.FramesIgnored.Load(),
		metrics.FramesCorrupt.Load(),
		metrics.FramesBackpressure.Load(),
	)
	fmt.Printf("Total Skor       : %d\n", totalScore)
}

func main() {
	runStressScenario("NetFlow Traffic Burst (1M Frames)", 100, 10_000, engine.Config{
		ChannelCapacity: 10_000,
		BatchSize:       512,
		FlushIntervalMs: 100,
	})
}
