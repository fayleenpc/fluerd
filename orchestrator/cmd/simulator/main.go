package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/fayleenpc/fluerd/pkg/common/configpb"
	"github.com/fayleenpc/fluerd/pkg/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	mode := flag.String("mode", "grpc", "Mode simulasi: grpc | udp | wave")
	workers := flag.Int("workers", 20, "Jumlah goroutine pengirim konkuren")
	total := flag.Int("total", 100_000, "Total frame yang dikirim")
	flag.Parse()

	perWorker := *total / *workers
	start := time.Now()

	switch *mode {
	case "grpc":
		runGRPCStream(*workers, perWorker)
	case "udp":
		runUDPFlood(*workers, perWorker)
	case "wave":
		runWaveTraffic(*workers, perWorker)
	default:
		panic("mode tidak dikenal, pilih: grpc | udp | wave")
	}

	elapsed := time.Since(start)
	fmt.Printf("\n=== SIMULASI [%s] SELESAI ===\n", *mode)
	fmt.Printf("Total Frame : %d | Waktu: %v | Rate: %.0f frame/detik\n",
		*workers*perWorker, elapsed, float64(*workers*perWorker)/elapsed.Seconds())
}

// 1. gRPC Client-Streaming (Protobuf -> gRPC -> Go Collector -> Rust FFI)
func runGRPCStream(workers, perWorker int) {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := configpb.NewPatroliIngestorClient(conn)
	payload := []byte{10, 20, 30, 40, 50, 60}
	var wg sync.WaitGroup

	for w := 1; w <= workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			stream, err := client.StreamTelemetry(context.Background())
			if err != nil {
				return
			}
			for i := 1; i <= perWorker; i++ {
				sev := uint32(3)
				if i%4 == 0 {
					sev = 8
				}
				_ = stream.Send(&configpb.TelemetryRequest{
					Severity: sev,
					DeviceId: fmt.Sprintf("grpc-edge-%d-%d", id, i),
					Payload:  payload,
				})
			}
			_, _ = stream.CloseAndRecv()
		}(w)
	}
	wg.Wait()
}

// 2. High-Frequency UDP Datagram Flood
func runUDPFlood(workers, perWorker int) {
	payload := []byte{15, 25, 35, 45}
	var wg sync.WaitGroup

	for w := 1; w <= workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.Dial("udp", "127.0.0.1:9000")
			if err != nil {
				return
			}
			defer conn.Close()

			buf := make([]byte, 128)
			prefix := []byte("udp-fw-" + strconv.Itoa(id) + "-")
			devID := make([]byte, 0, 48)

			for i := 1; i <= perWorker; i++ {
				sev := uint8(3)
				if i%5 == 0 {
					sev = 9
				}
				devID = append(devID[:0], prefix...)
				devID = strconv.AppendInt(devID, int64(i), 10)

				f := protocol.TelemetryFrame{Severity: sev, DeviceID: devID, Payload: payload}
				if n, err := f.EncodeInto(buf); err == nil {
					_, _ = conn.Write(buf[:n])
				}
			}
		}(w)
	}
	wg.Wait()
}

// 3. Bursty Wave Traffic (Menguji Timeout Flush saat jeda antar-gelombang)
func runWaveTraffic(workers, perWorker int) {
	conn, err := net.Dial("udp", "127.0.0.1:9000")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	buf := make([]byte, 128)
	payload := []byte{20, 40, 60}
	waves := 5
	perWave := (workers * perWorker) / waves

	for wave := 1; wave <= waves; wave++ {
		fmt.Printf("[Wave %d/%d] Mengirim burst %d paket...\n", wave, waves, perWave)
		for i := 1; i <= perWave; i++ {
			f := protocol.TelemetryFrame{
				Severity: 8,
				DeviceID: []byte("wave-sensor"),
				Payload:  payload,
			}
			if n, err := f.EncodeInto(buf); err == nil {
				_, _ = conn.Write(buf[:n])
			}
		}
		// Jeda 200ms (> 100ms FlushIntervalMs) untuk memaksa Rust mem-flush sisa batch via timeout
		time.Sleep(200 * time.Millisecond)
	}
}
