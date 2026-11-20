package transport

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/fayleenpc/fluerd/internal/shared/telemetry"
)

type UDPCollector struct {
	addr     string
	ingestor FrameIngestor
	metrics  *telemetry.Metrics
	bufPool  sync.Pool
}

func NewUDPCollector(addr string, ingestor FrameIngestor, metrics *telemetry.Metrics) *UDPCollector {
	return &UDPCollector{
		addr:     addr,
		ingestor: ingestor,
		metrics:  metrics,
		bufPool: sync.Pool{
			New: func() any {
				b := make([]byte, 2048)
				return &b
			},
		},
	}
}

func (s *UDPCollector) ListenAndServe(ctx context.Context) error {
	pc, err := net.ListenPacket("udp", s.addr)
	if err != nil {
		return err
	}
	defer pc.Close()

	go func() {
		<-ctx.Done()
		_ = pc.Close()
	}()

	for {
		bufPtr := s.bufPool.Get().(*[]byte)
		n, _, err := pc.ReadFrom(*bufPtr)
		if err != nil {
			s.bufPool.Put(bufPtr)
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		t0 := time.Now()
		status := s.ingestor.PushFrame((*bufPtr)[:n])
		if s.metrics != nil {
			s.metrics.ObservePush(int32(status), uint64(time.Since(t0).Nanoseconds()))
		}

		s.bufPool.Put(bufPtr)
	}
}
