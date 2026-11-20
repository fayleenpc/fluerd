package telemetry

import "net/http"

// DashboardHandler menyajikan UI observabilitas & simulator interaktif pada GET /.
func DashboardHandler(httpIngestURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>fluerd — Patroli Observability & FFI Console</title>
<style>
  :root { --bg: #0f172a; --card: #1e293b; --text: #f8fafc; --muted: #94a3b8; --accent: #38bdf8; --ok: #22c55e; --warn: #f59e0b; --err: #ef4444; }
  body { font-family: system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 24px; }
  .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; padding-bottom: 16px; margin-bottom: 24px; }
  .badge { background: #0284c7; padding: 4px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-bottom: 24px; }
  .card { background: var(--card); padding: 16px; border-radius: 10px; border: 1px solid #334155; }
  .card .label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; }
  .card .val { font-size: 28px; font-weight: 700; margin-top: 6px; font-variant-numeric: tabular-nums; }
  .controls { display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 24px; }
  button { background: #2563eb; color: white; border: none; padding: 10px 16px; border-radius: 8px; font-weight: 600; cursor: pointer; transition: opacity 0.15s; }
  button:hover { opacity: 0.9; }
  button.sec { background: #475569; }
  button.err { background: #dc2626; }
  button.burst { background: #059669; }
  table { width: 100%; border-collapse: collapse; background: var(--card); border-radius: 10px; overflow: hidden; }
  th, td { padding: 12px 14px; text-align: left; border-bottom: 1px solid #334155; font-size: 13px; }
  th { background: #0f172a; color: var(--muted); font-weight: 600; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
  .tag { display: inline-block; background: #334155; padding: 2px 6px; border-radius: 4px; margin-right: 4px; font-size: 11px; }
</style>
</head>
<body>
  <div class="header">
    <div>
      <h2 style="margin:0;">fluerd — Patroli Telemetry Console</h2>
      <span style="color:var(--muted); font-size:13px;">Go Multi-Protocol Collector &rarr; Rust Zero-Copy FFI Engine</span>
    </div>
    <span class="badge" id="statusBadge">LIVE • Auto-Refresh 1s</span>
  </div>

  <div class="controls">
    <button onclick="sendFrame('critical')">+ Kirim Paket Kritis (Sev 8)</button>
    <button class="sec" onclick="sendFrame('normal')">+ Kirim Paket Normal (Zero-Copy Drop)</button>
    <button class="err" onclick="sendFrame('corrupt')">+ Kirim Frame Rusak (Error)</button>
    <button class="burst" onclick="sendBurst(50)">&#9889; Burst 50 Paket Konkuren</button>
  </div>

  <div class="grid">
    <div class="card"><div class="label">Accepted (Masuk Rust)</div><div class="val" style="color:var(--ok)" id="mAccepted">0</div></div>
    <div class="card"><div class="label">Ignored (Zero-Copy)</div><div class="val" style="color:var(--accent)" id="mIgnored">0</div></div>
    <div class="card"><div class="label">Corrupt Frames</div><div class="val" style="color:var(--err)" id="mCorrupt">0</div></div>
    <div class="card"><div class="label">Backpressure Drop</div><div class="val" style="color:var(--warn)" id="mBackpressure">0</div></div>
    <div class="card"><div class="label">Avg FFI Latency</div><div class="val" id="mLatency">0 µs</div></div>
    <div class="card"><div class="label">Go Heap Memory</div><div class="val" id="mHeap">0 KB</div></div>
  </div>

  <h3 style="margin-bottom:12px;">Distributed Traces (Jaeger-Style Ring Buffer)</h3>
  <table>
    <thead>
      <tr>
        <th>Trace ID</th>
        <th>Span ID</th>
        <th>Operation</th>
        <th>Duration (µs)</th>
        <th>Tags</th>
      </tr>
    </thead>
    <tbody id="traceBody">
      <tr><td colspan="5" style="color:var(--muted); text-align:center;">Belum ada span. Klik tombol di atas untuk mengirim paket.</td></tr>
    </tbody>
  </table>

<script>
const INGEST_URL = "` + httpIngestURL + `/ingest";

function buildBinaryFrame(severity, deviceId, payloadBytes) {
  const enc = new TextEncoder();
  const idBytes = enc.encode(deviceId);
  const buf = new Uint8Array(3 + idBytes.length + payloadBytes.length);
  buf[0] = severity;
  buf[1] = (idBytes.length >> 8) & 0xff;
  buf[2] = idBytes.length & 0xff;
  buf.set(idBytes, 3);
  buf.set(payloadBytes, 3 + idBytes.length);
  return buf;
}

async function sendFrame(type) {
  let body;
  if (type === 'critical') {
    body = buildBinaryFrame(8, 'ui-gw-' + Math.floor(Math.random()*99), [10, 20, 30, 40, 50]);
  } else if (type === 'normal') {
    body = buildBinaryFrame(3, 'ui-edge-' + Math.floor(Math.random()*99), [10, 20]);
  } else {
    body = new Uint8Array([9, 0, 50, 98, 97, 100]);
  }
  await fetch(INGEST_URL, { method: 'POST', body });
  refreshData();
}

async function sendBurst(count) {
  const tasks = [];
  for (let i = 0; i < count; i++) {
    tasks.push(sendFrame(i % 5 === 0 ? 'critical' : 'normal'));
  }
  await Promise.all(tasks);
}

async function refreshData() {
  try {
    const [mRes, tRes] = await Promise.all([fetch('/metrics'), fetch('/traces')]);
    const mText = await mRes.text();
    const tJson = await tRes.json();

    const getMetric = (regex) => {
      const m = mText.match(regex);
      return m ? m[1] : '0';
    };

    document.getElementById('mAccepted').textContent = getMetric(/fluerd_frames_total\{status="accepted"\}\s+(\d+)/);
    document.getElementById('mIgnored').textContent = getMetric(/fluerd_frames_total\{status="ignored"\}\s+(\d+)/);
    document.getElementById('mCorrupt').textContent = getMetric(/fluerd_frames_total\{status="corrupt"\}\s+(\d+)/);
    document.getElementById('mBackpressure').textContent = getMetric(/fluerd_frames_total\{status="backpressure"\}\s+(\d+)/);
    document.getElementById('mLatency').textContent = getMetric(/fluerd_ffi_push_avg_microseconds\s+([\d.]+)/) + ' µs';

    const heapBytes = parseInt(getMetric(/go_heap_alloc_bytes\s+(\d+)/), 10);
    document.getElementById('mHeap').textContent = (heapBytes / 1024).toFixed(1) + ' KB';

    const tbody = document.getElementById('traceBody');
    if (tJson.spans && tJson.spans.length > 0) {
      const recent = [...tJson.spans].reverse().slice(0, 25);
      tbody.innerHTML = recent.map(s => {
        const tags = Object.entries(s.tags || {}).map(([k,v]) => '<span class="tag">' + k + '=' + v + '</span>').join('');
        return '<tr>' +
          '<td class="mono">' + s.traceID.slice(0, 16) + '...</td>' +
          '<td class="mono">' + s.spanID + '</td>' +
          '<td><b>' + s.operationName + '</b></td>' +
          '<td class="mono">' + s.durationUs + ' µs</td>' +
          '<td>' + tags + '</td>' +
        '</tr>';
      }).join('');
    }
  } catch (e) {
    document.getElementById('statusBadge').textContent = 'DISCONNECTED';
  }
}

setInterval(refreshData, 1000);
refreshData();
</script>
</body>
</html>`))
	}
}
