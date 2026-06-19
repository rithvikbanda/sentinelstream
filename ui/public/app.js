const POLL_INTERVAL_MS = 2000;
const STORAGE_KEY = 'sentinelstream.apiBase';

const apiBaseInput = document.getElementById('api-base');
const connStatus = document.getElementById('conn-status');
const metricsEl = document.getElementById('metrics');
const streamHealthEl = document.getElementById('stream-health');
const sensorsBody = document.getElementById('sensors-body');
const sensorCountEl = document.getElementById('sensor-count');
const lastUpdatedEl = document.getElementById('last-updated');

function apiBase() {
  return apiBaseInput.value.replace(/\/+$/, '');
}

apiBaseInput.value = localStorage.getItem(STORAGE_KEY) || window.SENTINELSTREAM_API_BASE || 'http://localhost:8080';
apiBaseInput.addEventListener('change', () => {
  localStorage.setItem(STORAGE_KEY, apiBaseInput.value);
});

async function fetchJSON(path) {
  const res = await fetch(`${apiBase()}${path}`);
  if (!res.ok) {
    throw new Error(`${path} -> HTTP ${res.status}`);
  }
  return res.json();
}

function card(label, value, tone) {
  const div = document.createElement('div');
  div.className = `card${tone ? ' ' + tone : ''}`;
  div.innerHTML = `<div class="label">${label}</div><div class="value">${value}</div>`;
  return div;
}

function renderMetrics(m) {
  metricsEl.replaceChildren(
    card('Received', m.messages_received),
    card('Processed', m.messages_processed),
    card('Queue depth', m.queue_depth),
    card('Healthy sensors', m.healthy_sensors, 'good'),
    card('Stale sensors', m.stale_sensors, m.stale_sensors > 0 ? 'warn' : ''),
    card('Validation errors', m.validation_errors, m.validation_errors > 0 ? 'bad' : ''),
    card('Processing errors', m.processing_errors, m.processing_errors > 0 ? 'bad' : ''),
    card('Anomalies', m.anomalies_detected, m.anomalies_detected > 0 ? 'warn' : ''),
    card('Dropped', m.dropped_messages, m.dropped_messages > 0 ? 'warn' : ''),
    card('Duplicates', m.duplicate_messages),
    card('Out of order', m.out_of_order_messages),
    card('Active TCP conns', m.active_tcp_connections),
  );
}

function renderStreamHealth(h) {
  streamHealthEl.replaceChildren(
    card('UDP received', h.udp.messages_received),
    card('UDP rejected', h.udp.messages_rejected, h.udp.messages_rejected > 0 ? 'bad' : ''),
    card('TCP received', h.tcp.messages_received),
    card('TCP rejected', h.tcp.messages_rejected, h.tcp.messages_rejected > 0 ? 'bad' : ''),
    card('TCP connections', `${h.tcp.active_connections} / ${h.tcp.total_connections}`),
    card('Queue', `${h.queue_depth} / ${h.queue_capacity}`),
    card('Workers running', h.workers_running ? 'yes' : 'no', h.workers_running ? 'good' : 'bad'),
  );
}

function statusPill(status) {
  return `<span class="pill ${status}">${status}</span>`;
}

function renderSensorRow(detail) {
  const tr = document.createElement('tr');
  const lastSeen = detail.last_seen ? new Date(detail.last_seen).toLocaleTimeString() : '-';
  tr.innerHTML = `
    <td>${detail.sensor_id}</td>
    <td>${detail.sensor_type}</td>
    <td>${statusPill(detail.status)}</td>
    <td>${detail.last_sequence}</td>
    <td>${detail.messages_received}</td>
    <td>${detail.messages_dropped}</td>
    <td>${detail.duplicates}</td>
    <td>${detail.out_of_order}</td>
    <td>${detail.errors}</td>
    <td>${detail.anomalies}</td>
    <td>${lastSeen}</td>
  `;
  return tr;
}

async function refreshSensors() {
  const sensors = await fetchJSON('/api/v1/sensors');
  sensorCountEl.textContent = `(${sensors.length})`;

  // The list endpoint only has id/type/status/last_seen; fetch each
  // sensor's full detail for the counters shown in the table. Fine for a
  // small local dashboard - not meant to scale to thousands of sensors.
  const details = await Promise.all(
    sensors.map((s) => fetchJSON(`/api/v1/sensors/${encodeURIComponent(s.sensor_id)}`).catch(() => null))
  );

  sensorsBody.replaceChildren(
    ...details.filter(Boolean).map(renderSensorRow)
  );
}

async function tick() {
  try {
    const [metrics, health] = await Promise.all([
      fetchJSON('/api/v1/metrics/summary'),
      fetchJSON('/api/v1/health/streams'),
    ]);
    renderMetrics(metrics);
    renderStreamHealth(health);
    await refreshSensors();

    connStatus.className = 'status-dot ok';
    lastUpdatedEl.textContent = `last updated ${new Date().toLocaleTimeString()}`;
  } catch (err) {
    connStatus.className = 'status-dot err';
    lastUpdatedEl.textContent = `error: ${err.message}`;
  }
}

tick();
setInterval(tick, POLL_INTERVAL_MS);
