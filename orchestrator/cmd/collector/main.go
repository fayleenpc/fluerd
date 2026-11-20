package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/fayleenpc/fluerd/internal/shared/telemetry"
	"github.com/fayleenpc/fluerd/internal/transport"
	"github.com/fayleenpc/fluerd/pkg/engine"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metrics := telemetry.NewMetrics()
	tracer := telemetry.NewTraceRegistry(200)

	ctx, rootSpan := telemetry.StartSpan(ctx, tracer, os.Stdout, "collector.lifecycle")
	rootSpan.SetTag("service", "fluerd-patroli-collector")

	eng, err := engine.New(engine.Config{
		ChannelCapacity: 10_000,
		BatchSize:       512,
		FlushIntervalMs: 100,
	})
	if err != nil {
		panic(err)
	}
	// Tentukan alamat HTTP Collector lebih awal agar bisa diteruskan ke UI Dashboard
	httpAddr := os.Getenv("HTTP_ADDR")
	if httpAddr == "" {
		httpAddr = ":8081"
	}

	var wg sync.WaitGroup

	// 1. Observability Server: Prometheus (/metrics) & Jaeger-style Ring Buffer (/traces)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", telemetry.DashboardHandler("http://localhost"+httpAddr))
	mux.HandleFunc("GET /metrics", metrics.PrometheusHandler())
	mux.HandleFunc("GET /traces", tracer.TracesHandler())

	metricsSrv := &http.Server{
		Addr:              ":9090",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("[Telemetry] Dashboard  : http://localhost:9090/")
		fmt.Println("[Telemetry] Prometheus : http://localhost:9090/metrics")
		fmt.Println("[Telemetry] Traces UI  : http://localhost:9090/traces")
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[Telemetry Error] %v\n", err)
		}
	}()

	// 2. UDP Collector (:9000)
	udpCollector := transport.NewUDPCollector(":9000", eng, metrics)
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("[Transport] UDP Collector mendengarkan di :9000")
		if err := udpCollector.ListenAndServe(ctx); err != nil {
			fmt.Printf("[UDP Error] %v\n", err)
		}
	}()

	// 3. HTTP Collector (default :8081 atau env HTTP_ADDR)
	httpCollector := transport.NewHTTPCollector(httpAddr, eng, metrics, tracer)

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Printf("[Transport] HTTP Collector aktif di %s (POST /ingest)\n", httpAddr)
		if err := httpCollector.ListenAndServe(ctx); err != nil {
			fmt.Printf("[HTTP Error] %v\n", err)
		}
	}()

	// 4. TCP/gRPC Stream Collector (:50051)
	// 4. gRPC Collector (:50051)
	grpcCollector := transport.NewGRPCCollector(":50051", eng, metrics, tracer)
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("[Transport] gRPC Collector aktif di :50051")
		if err := grpcCollector.ListenAndServe(ctx); err != nil {
			fmt.Printf("[gRPC Error] %v\n", err)
		}
	}()

	// Tunggu sinyal terminasi (SIGINT / SIGTERM)
	<-ctx.Done()
	fmt.Println("\n[Shutdown] Menerima sinyal terminasi, menghentikan semua listener...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)

	// Pastikan seluruh goroutine transport selesai sebelum menutup Rust FFI engine
	wg.Wait()

	finalScore := eng.Shutdown()
	metrics.LastAnomalyScore.Store(finalScore)
	rootSpan.SetTag("final_score", fmt.Sprintf("%d", finalScore))
	rootSpan.Finish()
	fmt.Printf("[Shutdown] Selesai bersih. Total Skor Anomali Akhir: %d\n", finalScore)
}
