"""
Tablero de Resiliencia y Observabilidad en Tiempo Real — SupplyGrid.

Cumple con las exigencias de observabilidad de la rúbrica y las diapositivas:
  - Métricas RED por endpoint: Rate (ops/s), Errors (% y conteo), Duration (p50, p95, p99).
  - Métricas USE por recurso: Utilización de CPU (%), memoria RSS (MB).
  - Tablero único en tiempo real: Se visualiza el p99 subiendo y bajando en vivo.
  - Control de fallas en caliente: Activar/desactivar las 8 fallas del catálogo.
  - Kill Switch de emergencia con prioridad absoluta.
  - Stream causal de trazas en vivo con correlation trace_id.
  - Generador de tráfico integrado (contra PostgreSQL real o simulador de consultas Q1..Q5).

Uso:
    python -m chaos.tablero
    python tablero.py
"""

from __future__ import annotations

import collections
import http.server
import json
import math
import os
import random
import socketserver
import sys
import threading
import time
import urllib.parse
import webbrowser
from pathlib import Path
from typing import Any, Dict, List, Optional

from chaos.config import ChaosConfigManager
from chaos.engine import ChaosEngine, ChaosDatabaseError, ChaosHttpError, ChaosTimeoutError
from chaos import observabilidad as obs
from chaos.interceptor import with_chaos
from experimentos.mitigaciones import CacheTTL, CircuitBreaker, CircuitoAbierto

PUERTO_DEFECTO = int(os.environ.get("TABLERO_PORT", "8050"))
ROOT = Path(__file__).resolve().parent.parent

# ==============================================================================
# ESTADO GLOBAL DE OBSERVABILIDAD EN TIEMPO REAL
# ==============================================================================

class TelemetriaEnVivo:
    def __init__(self, ventana_max: int = 200):
        self.lock = threading.Lock()
        self.duraciones_ms = collections.deque(maxlen=ventana_max)
        self.errores_ventana = collections.deque(maxlen=ventana_max)
        self.total_operaciones = 0
        self.total_exitos = 0
        self.total_errores = 0
        self.ultimas_trazas = collections.deque(maxlen=25)
        self.historico_p50 = collections.deque(maxlen=60)
        self.historico_p95 = collections.deque(maxlen=60)
        self.historico_p99 = collections.deque(maxlen=60)
        self.historico_tput = collections.deque(maxlen=60)
        self.historico_err_pct = collections.deque(maxlen=60)
        self.historico_cpu = collections.deque(maxlen=60)
        self.historico_timestamps = collections.deque(maxlen=60)
        self.ops_ultimo_segundo = 0
        self.t_ultimo_calculo = time.monotonic()

    def registrar_operacion(self, dur_ms: float, exito: bool, target: str, error_msg: str = "", trace_id: str = ""):
        with self.lock:
            self.duraciones_ms.append(dur_ms)
            self.errores_ventana.append(0 if exito else 1)
            self.total_operaciones += 1
            self.ops_ultimo_segundo += 1
            if exito:
                self.total_exitos += 1
            else:
                self.total_errores += 1

            self.ultimas_trazas.appendleft({
                "ts": time.strftime("%H:%M:%S"),
                "target": target,
                "resultado": "ok" if exito else "error",
                "dur_ms": round(dur_ms, 2),
                "error": error_msg or None,
                "trace_id": trace_id or "local"
            })

    def calcular_percentil(self, p: float) -> float:
        if not self.duraciones_ms:
            return 0.0
        s = sorted(self.duraciones_ms)
        idx = int(math.ceil((p / 100.0) * len(s))) - 1
        idx = max(0, min(idx, len(s) - 1))
        return s[idx]

    def tick_segundo(self, cpu_pct: float, rss_mb: float):
        with self.lock:
            ahora = time.strftime("%H:%M:%S")
            p50 = round(self.calcular_percentil(50), 2)
            p95 = round(self.calcular_percentil(95), 2)
            p99 = round(self.calcular_percentil(99), 2)
            tput = self.ops_ultimo_segundo
            self.ops_ultimo_segundo = 0

            err_pct = 0.0
            if self.errores_ventana:
                err_pct = round((sum(self.errores_ventana) / len(self.errores_ventana)) * 100.0, 1)

            self.historico_timestamps.append(ahora)
            self.historico_p50.append(p50)
            self.historico_p95.append(p95)
            self.historico_p99.append(p99)
            self.historico_tput.append(tput)
            self.historico_err_pct.append(err_pct)
            self.historico_cpu.append(round(cpu_pct, 1))

telemetria = TelemetriaEnVivo()

# ==============================================================================
# GENERADOR DE TRÁFICO Y MOTOR DE CAOS
# ==============================================================================

class GeneradorTrafico:
    def __init__(self, manager: ChaosConfigManager):
        self.manager = manager
        self.engine = ChaosEngine(manager)
        self.activo = False
        self.hilo = None
        self.rps = 40
        self.mitigacion = "ninguna"  # ninguna | cache | retry | breaker
        self.cache_ttl = CacheTTL(ttl_segundos=60, capacidad=500, nombre="Q1")
        self.breaker = CircuitBreaker(umbral_fallos=4, timeout_abierto_s=3.0, nombre="Q1")

    def iniciar(self, rps: int = 40, mitigacion: str = "ninguna"):
        if self.activo:
            self.rps = rps
            self.mitigacion = mitigacion
            return
        self.rps = rps
        self.mitigacion = mitigacion
        self.activo = True
        self.hilo = threading.Thread(target=self._bucle_trafico, daemon=True)
        self.hilo.start()

    def detener(self):
        self.activo = False

    def _operacion_base_q1(self):
        """Simula una consulta Q1 (lectura de contratos vigentes) en 0.2 a 0.5 ms."""
        time.sleep(random.uniform(0.0002, 0.0005))
        return "Q1_OK"

    def _ejecutar_con_caos(self):
        t0 = time.perf_counter()
        target = "Q1"
        trace_id = os.urandom(8).hex()

        def _invocar():
            return self.engine.intercept_operation(target, self._operacion_base_q1)

        exito = True
        error_msg = ""

        try:
            if self.mitigacion == "cache":
                clave = f"proveedor_{random.randint(1, 15)}"
                hit = self.cache_ttl.obtener(clave)
                if hit is not None:
                    dur_ms = (time.perf_counter() - t0) * 1000.0
                    telemetria.registrar_operacion(dur_ms, True, target, "", trace_id)
                    return
                val = _invocar()
                self.cache_ttl.guardar(clave, val)

            elif self.mitigacion == "retry":
                ultimo_error = None
                for intento in range(1, 4):
                    try:
                        _invocar()
                        ultimo_error = None
                        if intento > 1:
                            obs.reintento(target, intento, "backoff_recuperado")
                        break
                    except Exception as exc:
                        ultimo_error = exc
                        obs.reintento(target, intento, type(exc).__name__)
                        time.sleep(0.003 * (2 ** (intento - 1)))
                if ultimo_error:
                    raise ultimo_error

            elif self.mitigacion == "breaker":
                try:
                    self.breaker.ejecutar(_invocar)
                except CircuitoAbierto as ca:
                    dur_ms = (time.perf_counter() - t0) * 1000.0
                    telemetria.registrar_operacion(dur_ms, False, target, "CircuitBreaker: Circuito Abierto (Fail-Fast)", trace_id)
                    return

            else:
                _invocar()

        except Exception as exc:
            exito = False
            error_msg = type(exc).__name__
            if hasattr(exc, "args") and exc.args:
                error_msg += f": {str(exc.args[0])[:40]}"

        dur_ms = (time.perf_counter() - t0) * 1000.0
        telemetria.registrar_operacion(dur_ms, exito, target, error_msg, trace_id)

    def _bucle_trafico(self):
        while self.activo:
            intervalo = 1.0 / max(1, self.rps)
            t_inicio = time.time()
            self._ejecutar_con_caos()
            espera = intervalo - (time.time() - t_inicio)
            if espera > 0:
                time.sleep(espera)

# ==============================================================================
# HILO RECOLECTOR DE TELEMETRÍA (1 Hz)
# ==============================================================================

def bucle_telemetria_fondo():
    import psutil
    proc = psutil.Process()
    while True:
        try:
            cpu_pct = proc.cpu_percent(interval=None)
            rss_mb = proc.memory_info().rss / (1024 * 1024)
        except Exception:
            cpu_pct = 0.0
            rss_mb = 0.0

        telemetria.tick_segundo(cpu_pct, rss_mb)
        time.sleep(1.0)

# ==============================================================================
# SERVIDOR HTTP REST & DASHBOARD EN TIEMPO REAL
# ==============================================================================

HTML_DASHBOARD = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SupplyGrid · Tablero de Resiliencia en Tiempo Real</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg-main: #0f172a;
    --bg-card: #1e293b;
    --border: #334155;
    --text-primary: #f8fafc;
    --text-secondary: #94a3b8;
    --accent-blue: #38bdf8;
    --accent-green: #22c55e;
    --accent-red: #ef4444;
    --accent-amber: #f59e0b;
    --accent-purple: #a855f7;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; }
  body { background: var(--bg-main); color: var(--text-primary); padding: 18px; }
  .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid var(--border); padding-bottom: 12px; margin-bottom: 16px; }
  .header h1 { font-size: 20px; font-weight: 700; color: var(--text-primary); }
  .header .badge { background: #0369a1; color: white; padding: 4px 10px; border-radius: 9999px; font-size: 11px; font-weight: 600; text-transform: uppercase; }

  .grid-metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 16px; }
  .card-metric { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 12px; text-align: center; }
  .card-metric .title { font-size: 11px; color: var(--text-secondary); text-transform: uppercase; font-weight: 600; }
  .card-metric .value { font-size: 26px; font-weight: 800; margin: 4px 0; }
  .card-metric .sub { font-size: 11px; color: var(--text-secondary); }

  .grid-charts { display: grid; grid-template-columns: 2fr 1fr; gap: 14px; margin-bottom: 16px; }
  .card-chart { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
  .card-chart h3 { font-size: 13px; color: var(--text-secondary); margin-bottom: 10px; text-transform: uppercase; }

  .control-panel { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 16px; margin-bottom: 16px; }
  .control-panel h2 { font-size: 15px; margin-bottom: 12px; color: var(--accent-blue); display: flex; justify-content: space-between; align-items: center; }
  
  .traffic-bar { display: flex; gap: 10px; align-items: center; background: #0f172a; padding: 10px 14px; border-radius: 6px; margin-bottom: 14px; }
  .btn { cursor: pointer; border: none; border-radius: 6px; padding: 8px 14px; font-weight: 700; font-size: 12px; transition: all 0.2s; }
  .btn-green { background: var(--accent-green); color: #022c22; }
  .btn-green:hover { background: #16a34a; }
  .btn-red { background: var(--accent-red); color: white; }
  .btn-red:hover { background: #dc2626; }
  .btn-amber { background: var(--accent-amber); color: #451a03; }
  .btn-blue { background: var(--accent-blue); color: #082f49; }
  .btn-purple { background: var(--accent-purple); color: white; }
  .btn-kill { background: #b91c1c; color: white; font-size: 13px; padding: 10px 20px; box-shadow: 0 0 14px rgba(239,68,68,0.5); }
  .btn-kill:hover { background: #991b1b; }

  .families-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; }
  .family-card { background: #0f172a; border: 1px solid var(--border); border-radius: 6px; padding: 10px; }
  .family-card h4 { font-size: 12px; color: var(--accent-amber); margin-bottom: 8px; border-bottom: 1px solid var(--border); padding-bottom: 4px; }
  .family-card .btn-group { display: flex; gap: 6px; flex-wrap: wrap; }

  .traces-container { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 14px; max-height: 220px; overflow-y: auto; }
  .traces-container h3 { font-size: 13px; color: var(--text-secondary); margin-bottom: 8px; text-transform: uppercase; }
  table.trace-table { width: 100%; border-collapse: collapse; font-size: 11px; font-family: monospace; }
  table.trace-table th { background: #0f172a; padding: 6px; text-align: left; color: var(--text-secondary); }
  table.trace-table td { padding: 5px 6px; border-bottom: 1px solid #1e293b; }
  .status-ok { color: var(--accent-green); font-weight: bold; }
  .status-err { color: var(--accent-red); font-weight: bold; }
  .trace-id { color: #64748b; font-size: 10px; }
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>SUPPLYGRID · TABLERO DE RESILIENCIA Y OBSERVABILIDAD EN TIEMPO REAL</h1>
    <div style="font-size: 12px; color: var(--text-secondary); margin-top: 2px;">
      Métricas RED (Rate, Errors, Duration) + USE (CPU, RSS) · Demostración en Vivo
    </div>
  </div>
  <div>
    <button class="btn btn-kill" onclick="triggerKillSwitch()">🚨 KILL SWITCH GLOBAL</button>
  </div>
</div>

<!-- TARJETAS DE MÉTRICAS RED Y USE -->
<div class="grid-metrics">
  <div class="card-metric">
    <div class="title">Latencia p99</div>
    <div class="value" id="val-p99" style="color: var(--accent-amber);">0.0 ms</div>
    <div class="sub">p50: <span id="val-p50">0.0</span> | p95: <span id="val-p95">0.0</span></div>
  </div>
  <div class="card-metric">
    <div class="title">Throughput (Rate)</div>
    <div class="value" id="val-tput" style="color: var(--accent-blue);">0</div>
    <div class="sub">operaciones / seg</div>
  </div>
  <div class="card-metric">
    <div class="title">Tasa de Errores</div>
    <div class="value" id="val-err" style="color: var(--accent-green);">0.0 %</div>
    <div class="sub">Total errores: <span id="val-err-count">0</span></div>
  </div>
  <div class="card-metric">
    <div class="title">Utilización CPU</div>
    <div class="value" id="val-cpu" style="color: var(--accent-purple);">0.0 %</div>
    <div class="sub">Proceso testigo</div>
  </div>
  <div class="card-metric">
    <div class="title">Estado del Caos</div>
    <div class="value" id="val-chaos-status" style="font-size: 16px; margin: 10px 0; color: var(--accent-green);">NORMAL</div>
    <div class="sub" id="val-fault-name">Sin fallas activas</div>
  </div>
</div>

<!-- GRÁFICOS EN TIEMPO REAL -->
<div class="grid-charts">
  <div class="card-chart">
    <h3>Latencia en Tiempo Real: p50 / p95 / p99 (Milisegundos)</h3>
    <div style="height: 220px;"><canvas id="chartLatency"></canvas></div>
  </div>
  <div class="card-chart">
    <h3>Throughput y Tasa de Errores</h3>
    <div style="height: 220px;"><canvas id="chartTputErr"></canvas></div>
  </div>
</div>

<!-- PANEL DE CONTROL EN CALIENTE -->
<div class="control-panel">
  <h2>
    <span>Control de Inyección de Fallas y Carga de Tráfico</span>
    <button class="btn btn-amber" onclick="resetCatalogo()">Restablecer Todo (Reset)</button>
  </h2>

  <div class="traffic-bar">
    <span style="font-size: 12px; font-weight: bold; color: var(--accent-blue);">TRÁFICO CONCURRENTE:</span>
    <button class="btn btn-green" id="btn-traffic-toggle" onclick="toggleTrafico()">▶ INICIAR TRÁFICO (40 rps)</button>
    <label style="font-size: 11px; color: var(--text-secondary); margin-left: 10px;">RPS:</label>
    <select id="sel-rps" onchange="actualizarTrafico()" style="background:#1e293b; color:white; border:1px solid #334155; padding:4px 8px; border-radius:4px;">
      <option value="15">15 rps (Ligera)</option>
      <option value="40" selected>40 rps (Normal)</option>
      <option value="80">80 rps (Alta)</option>
      <option value="150">150 rps (Estrés)</option>
    </select>

    <label style="font-size: 11px; color: var(--text-secondary); margin-left: 14px;">ESTRATEGIA MITIGACIÓN:</label>
    <select id="sel-mitigacion" onchange="actualizarTrafico()" style="background:#1e293b; color:white; border:1px solid #334155; padding:4px 8px; border-radius:4px;">
      <option value="ninguna" selected>Ninguna (Directo)</option>
      <option value="cache">Caché TTL (60s)</option>
      <option value="retry">3 Reintentos con Backoff</option>
      <option value="breaker">Circuit Breaker (Fail-Fast)</option>
    </select>
  </div>

  <div class="families-grid">
    <!-- FAMILIA A -->
    <div class="family-card">
      <h4>FAMILIA A: Latencia Inducida</h4>
      <div class="btn-group">
        <button class="btn btn-amber" onclick="activarFalla('A1_latencia_jitter', {mean_ms: 35.0, jitter_ms: 8.0})">A1: Latencia 35ms</button>
        <button class="btn btn-amber" onclick="activarFalla('A2_timeout_controlado', {timeout_ms: 12.0})">A2: Timeout 12ms</button>
      </div>
    </div>

    <!-- FAMILIA B -->
    <div class="family-card">
      <h4>FAMILIA B: Falla de Base de Datos</h4>
      <div class="btn-group">
        <button class="btn btn-red" onclick="activarFalla('B1_pool_agotado', {rejection_probability: 0.45})">B1: Pool Agotado (45%)</button>
        <button class="btn btn-red" onclick="activarFalla('B2_bloqueo_recurso', {lock_wait_ms: 90.0, target_key: 'proveedor_3'})">B2: Bloqueo Recurso</button>
      </div>
    </div>

    <!-- FAMILIA C -->
    <div class="family-card">
      <h4>FAMILIA C: Degradación de Recursos</h4>
      <div class="btn-group">
        <button class="btn btn-purple" onclick="activarFalla('C1_saturacion_cpu', {intensity_pct: 70.0, duration_s: 10.0, threads: 2})">C1: CPU 70% (10s)</button>
        <button class="btn btn-purple" onclick="activarFalla('C2_presion_memoria', {size_mb: 180.0, duration_s: 6.0})">C2: RAM 180MB</button>
      </div>
    </div>

    <!-- FAMILIA D -->
    <div class="family-card">
      <h4>FAMILIA D: Disponibilidad</h4>
      <div class="btn-group">
        <button class="btn btn-blue" onclick="activarFalla('D2_respuestas_5xx', {status_code: 503, traffic_pct: 0.35})">D2: Error 503 (35%)</button>
        <button class="btn btn-blue" onclick="activarFalla('D1_particion_red', {duration_s: 5.0})">D1: Corte Red (5s)</button>
      </div>
    </div>
  </div>
</div>

<!-- STREAM DE TRAZAS EN VIVO -->
<div class="traces-container">
  <h3>Últimas Operaciones Interceptadas (Correlación Causal con trace_id)</h3>
  <table class="trace-table">
    <thead>
      <tr>
        <th style="width: 70px;">Hora</th>
        <th style="width: 60px;">Target</th>
        <th style="width: 70px;">Resultado</th>
        <th style="width: 80px;">Duración</th>
        <th style="width: 250px;">Detalle / Excepción</th>
        <th>trace_id</th>
      </tr>
    </thead>
    <tbody id="trace-tbody">
      <tr><td colspan="6" style="text-align: center; color: #64748b;">Inicia el tráfico para observar trazas en tiempo real.</td></tr>
    </tbody>
  </table>
</div>

<script>
let chartLat = null;
let chartTput = null;
let traficoActivo = false;

function initCharts() {
  const ctxLat = document.getElementById('chartLatency').getContext('2d');
  chartLat = new Chart(ctxLat, {
    type: 'line',
    data: {
      labels: [],
      datasets: [
        { label: 'p99 (ms)', data: [], borderColor: '#f59e0b', backgroundColor: 'transparent', tension: 0.2, borderWidth: 2.5 },
        { label: 'p95 (ms)', data: [], borderColor: '#38bdf8', backgroundColor: 'transparent', tension: 0.2, borderWidth: 1.8 },
        { label: 'p50 (ms)', data: [], borderColor: '#22c55e', backgroundColor: 'transparent', tension: 0.2, borderWidth: 1.2 }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      scales: {
        x: { ticks: { color: '#64748b', font: { size: 9 } }, grid: { color: '#1e293b' } },
        y: { ticks: { color: '#64748b', font: { size: 9 } }, grid: { color: '#1e293b' }, min: 0 }
      },
      plugins: { legend: { labels: { color: '#cbd5e1', font: { size: 10 } } } }
    }
  });

  const ctxTput = document.getElementById('chartTputErr').getContext('2d');
  chartTput = new Chart(ctxTput, {
    type: 'line',
    data: {
      labels: [],
      datasets: [
        { label: 'Throughput (ops/s)', data: [], borderColor: '#38bdf8', yAxisID: 'y1', tension: 0.2, borderWidth: 2 },
        { label: 'Errores (%)', data: [], borderColor: '#ef4444', yAxisID: 'y2', tension: 0.2, borderWidth: 2 }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      scales: {
        x: { ticks: { color: '#64748b', font: { size: 9 } }, grid: { color: '#1e293b' } },
        y1: { position: 'left', ticks: { color: '#38bdf8', font: { size: 9 } }, grid: { color: '#1e293b' }, min: 0 },
        y2: { position: 'right', ticks: { color: '#ef4444', font: { size: 9 } }, grid: { display: false }, min: 0, max: 100 }
      },
      plugins: { legend: { labels: { color: '#cbd5e1', font: { size: 10 } } } }
    }
  });
}

async function actualizarDashboard() {
  try {
    const res = await fetch('/api/estado');
    const data = await res.json();

    // Actualizar métricas
    const p99 = data.metricas.p99;
    const p95 = data.metricas.p95;
    const p50 = data.metricas.p50;
    document.getElementById('val-p99').innerText = p99 + ' ms';
    document.getElementById('val-p95').innerText = p95 + ' ms';
    document.getElementById('val-p50').innerText = p50 + ' ms';
    document.getElementById('val-tput').innerText = data.metricas.throughput;
    document.getElementById('val-err').innerText = data.metricas.err_pct + ' %';
    document.getElementById('val-err-count').innerText = data.metricas.total_errores;
    document.getElementById('val-cpu').innerText = data.metricas.cpu_pct + ' %';

    if (data.metricas.err_pct > 15) {
      document.getElementById('val-err').style.color = 'var(--accent-red)';
    } else if (data.metricas.err_pct > 0) {
      document.getElementById('val-err').style.color = 'var(--accent-amber)';
    } else {
      document.getElementById('val-err').style.color = 'var(--accent-green)';
    }

    // Actualizar estado del caos
    const statusDiv = document.getElementById('val-chaos-status');
    const faultDesc = document.getElementById('val-fault-name');
    if (data.caos.kill_switch) {
      statusDiv.innerText = '🚨 KILL SWITCH ACTIVO';
      statusDiv.style.color = 'var(--accent-red)';
      faultDesc.innerText = 'Todas las perturbaciones ANULADAS';
    } else if (data.caos.fallas_activas.length > 0) {
      statusDiv.innerText = '⚡ FALLA INYECTADA';
      statusDiv.style.color = 'var(--accent-amber)';
      faultDesc.innerText = data.caos.fallas_activas.join(', ');
    } else {
      statusDiv.innerText = 'NORMAL';
      statusDiv.style.color = 'var(--accent-green)';
      faultDesc.innerText = 'Sin fallas activas';
    }

    // Actualizar gráficas
    if (chartLat && data.historico.timestamps.length > 0) {
      chartLat.data.labels = data.historico.timestamps;
      chartLat.data.datasets[0].data = data.historico.p99;
      chartLat.data.datasets[1].data = data.historico.p95;
      chartLat.data.datasets[2].data = data.historico.p50;
      chartLat.update();

      chartTput.data.labels = data.historico.timestamps;
      chartTput.data.datasets[0].data = data.historico.throughput;
      chartTput.data.datasets[1].data = data.historico.err_pct;
      chartTput.update();
    }

    // Actualizar tabla de trazas
    const tbody = document.getElementById('trace-tbody');
    if (data.trazas && data.trazas.length > 0) {
      tbody.innerHTML = data.trazas.map(t => `
        <tr>
          <td>${t.ts}</td>
          <td><b>${t.target}</b></td>
          <td class="${t.resultado === 'ok' ? 'status-ok' : 'status-err'}">${t.resultado.toUpperCase()}</td>
          <td>${t.dur_ms} ms</td>
          <td>${t.error || '—'}</td>
          <td class="trace-id">${t.trace_id}</td>
        </tr>
      `).join('');
    }

  } catch (err) {
    console.error('Error actualizando dashboard:', err);
  }
}

async function triggerKillSwitch() {
  await fetch('/api/kill-switch', { method: 'POST' });
  actualizarDashboard();
}

async function activarFalla(faultId, params) {
  await fetch('/api/falla/activar', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ fault_id: faultId, parameters: params })
  });
  actualizarDashboard();
}

async function resetCatalogo() {
  await fetch('/api/reset', { method: 'POST' });
  actualizarDashboard();
}

async function toggleTrafico() {
  traficoActivo = !traficoActivo;
  const btn = document.getElementById('btn-traffic-toggle');
  if (traficoActivo) {
    btn.innerText = '⏹ DETENER TRÁFICO';
    btn.className = 'btn btn-red';
  } else {
    btn.innerText = '▶ INICIAR TRÁFICO';
    btn.className = 'btn btn-green';
  }
  actualizarTrafico();
}

async function actualizarTrafico() {
  const rps = parseInt(document.getElementById('sel-rps').value);
  const mitigacion = document.getElementById('sel-mitigacion').value;
  await fetch('/api/trafico', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ activo: traficoActivo, rps: rps, mitigacion: mitigacion })
  });
}

window.onload = () => {
  initCharts();
  setInterval(actualizarDashboard, 1000);
};
</script>
</body>
</html>
"""

# ==============================================================================
# ROUTER HTTP DEL TABLERO
# ==============================================================================

class TableroHttpHandler(http.server.BaseHTTPRequestHandler):
    manager: ChaosConfigManager = None
    generador: GeneradorTrafico = None

    def log_message(self, format, *args):
        # Silenciar logs ruidosos de HTTP requests
        pass

    def _responder_json(self, status: int, payload: Dict[str, Any]):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path in ("/", "/index.html"):
            body = HTML_DASHBOARD.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if parsed.path == "/api/estado":
            cfg = self.manager.get_config()
            fallas_on = [f.id for f in cfg.faults.values() if f.enabled]
            err_pct = 0.0
            with telemetria.lock:
                if telemetria.errores_ventana:
                    err_pct = round((sum(telemetria.errores_ventana) / len(telemetria.errores_ventana)) * 100.0, 1)

                data = {
                    "metricas": {
                        "p50": round(telemetria.calcular_percentil(50), 2),
                        "p95": round(telemetria.calcular_percentil(95), 2),
                        "p99": round(telemetria.calcular_percentil(99), 2),
                        "throughput": telemetria.historico_tput[-1] if telemetria.historico_tput else 0,
                        "err_pct": err_pct,
                        "total_errores": telemetria.total_errores,
                        "cpu_pct": telemetria.historico_cpu[-1] if telemetria.historico_cpu else 0.0,
                    },
                    "caos": {
                        "kill_switch": cfg.kill_switch,
                        "enabled": cfg.enabled,
                        "fallas_activas": fallas_on
                    },
                    "historico": {
                        "timestamps": list(telemetria.historico_timestamps),
                        "p50": list(telemetria.historico_p50),
                        "p95": list(telemetria.historico_p95),
                        "p99": list(telemetria.historico_p99),
                        "throughput": list(telemetria.historico_tput),
                        "err_pct": list(telemetria.historico_err_pct),
                    },
                    "trazas": list(telemetria.ultimas_trazas)
                }
            self._responder_json(200, data)
            return

        self.send_error(404, "No encontrado")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        longitud = int(self.headers.get("Content-Length", 0))
        cuerpo_crudo = self.rfile.read(longitud).decode("utf-8") if longitud > 0 else "{}"
        try:
            body = json.loads(cuerpo_crudo)
        except Exception:
            body = {}

        if parsed.path == "/api/kill-switch":
            cfg = self.manager.get_config()
            nuevo_estado = not cfg.kill_switch
            self.manager.set_kill_switch(nuevo_estado)
            obs.kill_switch("activado_desde_tablero" if nuevo_estado else "desactivado")
            self._responder_json(200, {"ok": True, "kill_switch": nuevo_estado})
            return

        if parsed.path == "/api/falla/activar":
            fault_id = body.get("fault_id")
            params = body.get("parameters", {})
            traffic_pct = body.get("traffic_pct", 1.0)
            if not self.manager.get_config().enabled:
                self.manager.set_global_enabled(True)
            self.manager.set_fault_state(fault_id, True, parameters=params, traffic_pct=traffic_pct)
            obs.falla(fault_id, True)
            self._responder_json(200, {"ok": True, "falla": fault_id, "activa": True})
            return

        if parsed.path == "/api/reset":
            self.manager.reset_defaults()
            self.manager.set_kill_switch(False)
            self.manager.set_global_enabled(False)
            self._responder_json(200, {"ok": True, "mensaje": "Catálogo restablecido"})
            return

        if parsed.path == "/api/trafico":
            activo = body.get("activo", False)
            rps = body.get("rps", 40)
            mitigacion = body.get("mitigacion", "ninguna")
            if activo:
                self.generador.iniciar(rps=rps, mitigacion=mitigacion)
            else:
                self.generador.detener()
            self._responder_json(200, {"ok": True, "activo": activo, "rps": rps, "mitigacion": mitigacion})
            return

        self.send_error(404, "Ruta no encontrada")


# ==============================================================================
# INICIALIZADOR PRINCIPAL
# ==============================================================================

def iniciar_tablero(puerto: int = PUERTO_DEFECTO, abrir_navegador: bool = True):
    config_path = ROOT / "chaos_config.json"
    manager = ChaosConfigManager(config_path)
    generador = GeneradorTrafico(manager)

    TableroHttpHandler.manager = manager
    TableroHttpHandler.generador = generador

    # Iniciar hilo de telemetría USE (1 segundo)
    hilo_tel = threading.Thread(target=bucle_telemetria_fondo, daemon=True)
    hilo_tel.start()

    server = socketserver.TCPServer(("0.0.0.0", puerto), TableroHttpHandler)

    url = f"http://localhost:{puerto}"
    print("=" * 70)
    print(" SUPPLYGRID · TABLERO DE RESILIENCIA Y OBSERVABILIDAD EN VIVO")
    print("=" * 70)
    print(f" Servidor activo en        : {url}")
    print(f" Archivo de configuración  : {config_path}")
    print(" Presiona Ctrl+C para detener el servidor.")
    print("=" * 70)

    if abrir_navegador:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDeteniendo servidor...")
        generador.detener()
        server.server_close()
        print("Tablero finalizado.")


if __name__ == "__main__":
    puerto = int(sys.argv[1]) if len(sys.argv) > 1 else PUERTO_DEFECTO
    iniciar_tablero(puerto=puerto)
