package transport

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/fayleenpc/fluerd/internal/shared/telemetry"
	"github.com/fayleenpc/fluerd/pkg/common/configpb"
	"github.com/fayleenpc/fluerd/pkg/engine"
	"github.com/fayleenpc/fluerd/pkg/protocol"
	"google.golang.org/grpc"
)

type GRPCCollector struct {
	configpb.UnimplementedPatroliIngestorServer
	addr     string
	ingestor FrameIngestor
	metrics  *telemetry.Metrics
	tracer   *telemetry.TraceRegistry
	server   *grpc.Server
	bufPool  sync.Pool
}

func NewGRPCCollector(
	addr string,
	ingestor FrameIngestor,
	metrics *telemetry.Metrics,
	tracer *telemetry.TraceRegistry,
) *GRPCCollector {
	g := &GRPCCollector{
		addr:     addr,
		ingestor: ingestor,
		metrics:  metrics,
		tracer:   tracer,
		server:   grpc.NewServer(),
		bufPool: sync.Pool{
			New: func() any {
				b := make([]byte, 64*1024)
				return &b
			},
		},
	}
	configpb.RegisterPatroliIngestorServer(g.server, g)
	return g
}

func (g *GRPCCollector) processProto(ctx context.Context, req *configpb.TelemetryRequest, op string) *configpb.IngestResponse {
	_, span := telemetry.StartSpan(ctx, g.tracer, nil, op)
	span.SetTag("protocol", "grpc")
	span.SetTag("device_id", req.GetDeviceId())
	defer span.Finish()

	bufPtr := g.bufPool.Get().(*[]byte)
	defer g.bufPool.Put(bufPtr)

	frame := protocol.TelemetryFrame{
		Severity: uint8(req.GetSeverity()),
		DeviceID: []byte(req.GetDeviceId()),
		Payload:  req.GetPayload(),
	}

	n, err := frame.EncodeInto(*bufPtr)
	if err != nil {
		span.SetTag("error", err.Error())
		return &configpb.IngestResponse{StatusCode: int32(engine.StatusCorrupt), StatusLabel: "CORRUPT"}
	}

	t0 := time.Now()
	status := g.ingestor.PushFrame((*bufPtr)[:n])
	if g.metrics != nil {
		g.metrics.ObservePush(int32(status), uint64(time.Since(t0).Nanoseconds()))
	}
	span.SetTag("ffi_status", fmt.Sprintf("%d", status))

	return &configpb.IngestResponse{
		StatusCode:  int32(status),
		StatusLabel: statusLabel(status),
	}
}

func (g *GRPCCollector) PushTelemetry(ctx context.Context, req *configpb.TelemetryRequest) (*configpb.IngestResponse, error) {
	return g.processProto(ctx, req, "grpc.unary_ingest"), nil
}

func (g *GRPCCollector) StreamTelemetry(stream configpb.PatroliIngestor_StreamTelemetryServer) error {
	var processed int32
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&configpb.IngestResponse{
				StatusCode:  processed,
				StatusLabel: "STREAM_COMPLETED",
			})
		}
		if err != nil {
			return err
		}
		_ = g.processProto(stream.Context(), req, "grpc.stream_ingest")
		processed++
	}
}

func (g *GRPCCollector) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", g.addr)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		g.server.GracefulStop()
	}()

	return g.server.Serve(ln)
}

func statusLabel(s engine.PushStatus) string {
	switch s {
	case engine.StatusAccepted:
		return "ACCEPTED"
	case engine.StatusIgnored:
		return "IGNORED"
	case engine.StatusBackpressure:
		return "BACKPRESSURE"
	default:
		return "CORRUPT"
	}
}
