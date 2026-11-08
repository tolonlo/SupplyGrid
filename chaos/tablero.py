"""
Tablero de Resiliencia, Observabilidad y Portal B2B en Tiempo Real — SupplyGrid.

Integra:
  1. Portal B2B de Negocio (Pedidos, Proveedores, ETAs, SLA de Entrega, KPIs comerciales).
  2. Métricas RED por endpoint: Rate (ops/s), Errors (% y conteo), Duration (p50, p95, p99).
  3. Métricas USE por recurso: Utilización de CPU (%), memoria RSS (MB).
  4. Control de fallas en caliente (A1, A2, B1 45%, B2, C1 70%, C2 RAM, D1 corte 5s, D2 503 35%).
  5. Auto-recuperación temporal en fallas transitorias (D1 se cura a los 5s, C1 a los 15s).
  6. Kill Switch global de emergencia con prioridad absoluta.
  7. Auditoría de trazas causales con correlación trace_id.
  8. Tráfico baseline automático (40 rps) listo desde el arranque.

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
# CATÁLOGO DE PROVEEDORES Y PRODUCTOS B2B (Basado en arquitectura SupplyGrid)
# ==============================================================================

PROVEEDORES_B2B = [
    {
        "id": 1,
        "nombre": "Logística & Cargas del Café S.A.S.",
        "nit": "900.123.456-1",
        "ciudad": "Medellín",
        "sla_pct": 99.2,
        "calificacion": 4.9,
        "contrato": "VIGENTE",
        "tiempo_resp_ms": "0.4 ms",
        "es_lider_zipf": True
    },
    {
        "id": 2,
        "nombre": "BioFarma Insumos Industriales S.A.",
        "nit": "811.987.654-3",
        "ciudad": "Bogotá D.C.",
        "sla_pct": 98.8,
        "calificacion": 5.0,
        "contrato": "VIGENTE",
        "tiempo_resp_ms": "0.6 ms",
        "es_lider_zipf": True
    },
    {
        "id": 3,
        "nombre": "Empaques & Polímeros de Occidente",
        "nit": "890.321.789-5",
        "ciudad": "Cali",
        "sla_pct": 97.5,
        "calificacion": 4.7,
        "contrato": "VIGENTE",
        "tiempo_resp_ms": "0.8 ms",
        "es_lider_zipf": False
    },
    {
        "id": 4,
        "nombre": "Química Global del Caribe S.A.",
        "nit": "800.456.789-2",
        "ciudad": "Barranquilla",
        "sla_pct": 98.1,
        "calificacion": 4.8,
        "contrato": "VIGENTE",
        "tiempo_resp_ms": "0.7 ms",
        "es_lider_zipf": False
    },
    {
        "id": 5,
        "nombre": "TechSupply Hardware B2B S.A.S.",
        "nit": "901.654.321-8",
        "ciudad": "Manizales",
        "sla_pct": 99.4,
        "calificacion": 4.9,
        "contrato": "VIGENTE",
        "tiempo_resp_ms": "0.5 ms",
        "es_lider_zipf": False
    }
]

PRODUCTOS_B2B = [
    {"sku": "SKU-BIO-042", "nombre": "Pallet Insumo Químico Grado A", "precio": 12500000, "eta": "18 hrs (Ruta Expreso)"},
    {"sku": "SKU-LOG-119", "nombre": "Contenedor Material Termorregulable", "precio": 28400000, "eta": "36 hrs (Consolidación)"},
    {"sku": "SKU-EMP-883", "nombre": "Bobinas de Polietileno Biodegradable (x50)", "precio": 8900000, "eta": "24 hrs (Ruta Directa)"},
    {"sku": "SKU-TEC-501", "nombre": "Sensores IoT de Cadena de Frío B2B", "precio": 16200000, "eta": "12 hrs (Despacho Aéreo)"},
    {"sku": "SKU-QUI-772", "nombre": "Reactivo Analítico Certificado ISO", "precio": 34100000, "eta": "48 hrs (Transporte Especial)"}
]


# ==============================================================================
# ESTADO GLOBAL DE OBSERVABILIDAD Y NEGOCIO EN TIEMPO REAL
# ==============================================================================

class TelemetriaEnVivo:
    def __init__(self, ventana_max: int = 200):
        self.lock = threading.Lock()
        self.duraciones_ms = collections.deque(maxlen=ventana_max)
        self.errores_ventana = collections.deque(maxlen=ventana_max)
        self.total_operaciones = 0
        self.total_exitos = 0
        self.total_errores = 0
        self.total_facturado_cop = 1450000000
        self.ultimas_trazas = collections.deque(maxlen=30)
        self.ultimos_pedidos = collections.deque(maxlen=15)
        self.historico_p50 = collections.deque(maxlen=60)
        self.historico_p95 = collections.deque(maxlen=60)
        self.historico_p99 = collections.deque(maxlen=60)
        self.historico_tput = collections.deque(maxlen=60)
        self.historico_err_pct = collections.deque(maxlen=60)
        self.historico_cpu = collections.deque(maxlen=60)
        self.historico_rss = collections.deque(maxlen=60)
        self.historico_timestamps = collections.deque(maxlen=60)
        self.ops_ultimo_segundo = 0
        self.t_ultimo_calculo = time.monotonic()
        self._contador_pedidos = 10420

    def registrar_transaccion_b2b(
        self,
        dur_ms: float,
        exito: bool,
        target: str,
        error_msg: str = "",
        trace_id: str = "",
        datos_pedido: Optional[Dict[str, Any]] = None
    ):
        with self.lock:
            self.duraciones_ms.append(dur_ms)
            self.errores_ventana.append(0 if exito else 1)
            self.total_operaciones += 1
            self.ops_ultimo_segundo += 1

            if exito:
                self.total_exitos += 1
                if datos_pedido:
                    self.total_facturado_cop += datos_pedido.get("precio", 0)
            else:
                self.total_errores += 1

            ahora_str = time.strftime("%H:%M:%S")

            # Traza técnica de observabilidad (Persona B / Causal)
            self.ultimas_trazas.appendleft({
                "ts": ahora_str,
                "target": target,
                "resultado": "ok" if exito else "error",
                "dur_ms": round(dur_ms, 2),
                "error": error_msg or None,
                "trace_id": trace_id or "local"
            })

            # Si es una transacción que generó o impactó un pedido B2B
            if datos_pedido:
                estado_negocio = "CONFIRMADA" if exito else "FALLIDA"
                if not exito:
                    if "Pool" in error_msg:
                        estado_negocio = "FALLA_DB (Pool Agotado)"
                    elif "503" in error_msg or "Http" in error_msg:
                        estado_negocio = "ERROR_503 (Pasarela Externa)"
                    elif "Partición" in error_msg or "red" in error_msg.lower():
                        estado_negocio = "FALLA_RED (Socket Cerrado)"
                    elif "Timeout" in error_msg:
                        estado_negocio = "TIMEOUT (SLA Excedido)"
                    elif "CircuitBreaker" in error_msg:
                        estado_negocio = "BLOQUEADA (Circuit Breaker)"
                    else:
                        estado_negocio = "RECHAZADA"

                self.ultimos_pedidos.appendleft({
                    "id": datos_pedido["id"],
                    "hora": ahora_str,
                    "proveedor": datos_pedido["proveedor"],
                    "producto": datos_pedido["producto"],
                    "sku": datos_pedido["sku"],
                    "monto_fmt": f"$ {datos_pedido['precio']:,.0f} COP",
                    "eta": datos_pedido["eta"],
                    "estado": estado_negocio,
                    "exito": exito,
                    "dur_ms": round(dur_ms, 2),
                    "trace_id": trace_id
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
            self.historico_rss.append(round(rss_mb, 1))


telemetria = TelemetriaEnVivo()


# ==============================================================================
# GENERADOR DE TRÁFICO Y SIMULADOR DE MÓDULOS B2B
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

    def _generar_pedido_aleatorio(self) -> Dict[str, Any]:
        # Distribución Zipf: 40% de órdenes van a proveedores líderes (1 o 2)
        if random.random() < 0.40:
            prov = random.choice([PROVEEDORES_B2B[0], PROVEEDORES_B2B[1]])
        else:
            prov = random.choice(PROVEEDORES_B2B)

        prod = random.choice(PRODUCTOS_B2B)
        telemetria._contador_pedidos += 1
        return {
            "id": f"ORD-{telemetria._contador_pedidos}",
            "proveedor": prov["nombre"],
            "producto": prod["nombre"],
            "sku": prod["sku"],
            "precio": prod["precio"],
            "eta": prod["eta"],
        }

    def _operacion_base_b2b(self, target: str):
        """Simula tiempo de procesamiento en módulos Spring Modulith (0.2 a 0.6 ms)."""
        time.sleep(random.uniform(0.0002, 0.0006))
        return f"{target}_OK"

    def ejecutar_transaccion(self, target: str = "Q1_contratos_proveedor", datos_pedido: Optional[Dict[str, Any]] = None):
        t0 = time.perf_counter()
        trace_id = os.urandom(8).hex()

        def _invocar():
            return self.engine.intercept_operation(target, lambda: self._operacion_base_b2b(target))

        exito = True
        error_msg = ""

        try:
            if self.mitigacion == "cache" and target == "Q1_contratos_proveedor":
                clave = f"proveedor_{random.randint(1, 10)}"
                hit = self.cache_ttl.obtener(clave)
                if hit is not None:
                    dur_ms = (time.perf_counter() - t0) * 1000.0
                    telemetria.registrar_transaccion_b2b(dur_ms, True, target, "", trace_id, datos_pedido)
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
                except CircuitoAbierto:
                    dur_ms = (time.perf_counter() - t0) * 1000.0
                    telemetria.registrar_transaccion_b2b(
                        dur_ms, False, target, "CircuitBreaker: Circuito Abierto (Fail-Fast)", trace_id, datos_pedido
                    )
                    return

            else:
                _invocar()

        except Exception as exc:
            exito = False
            error_msg = type(exc).__name__
            if hasattr(exc, "args") and exc.args:
                error_msg += f": {str(exc.args[0])[:45]}"

        dur_ms = (time.perf_counter() - t0) * 1000.0
        telemetria.registrar_transaccion_b2b(dur_ms, exito, target, error_msg, trace_id, datos_pedido)

    def _bucle_trafico(self):
        targets = [
            "Q1_contratos_proveedor",
            "Q2_catalogo_sku",
            "Q3_ordenes_compra",
            "Q5_despacho_logistica",
            "ext_facturacion_tax"
        ]
        while self.activo:
            intervalo = 1.0 / max(1, self.rps)
            t_inicio = time.time()

            target = random.choice(targets)
            # Solo un subconjunto de transacciones genera órdenes completas para no saturar la tabla B2B
            datos_pedido = self._generar_pedido_aleatorio() if random.random() < 0.25 else None

            self.ejecutar_transaccion(target, datos_pedido)

            espera = intervalo - (time.time() - t_inicio)
            if espera > 0:
                time.sleep(espera)


# ==============================================================================
# HILO RECOLECTOR DE TELEMETRÍA USE (1 Hz)
# ==============================================================================

def bucle_telemetria_fondo():
    import psutil
    proc = psutil.Process()
    proc.cpu_percent(interval=None)
    while True:
        time.sleep(1.0)
        try:
            cpu_pct = proc.cpu_percent(interval=None)
            rss_mb = proc.memory_info().rss / (1024 * 1024)
        except Exception:
            cpu_pct = 0.0
            rss_mb = 0.0

        telemetria.tick_segundo(cpu_pct, rss_mb)


# ==============================================================================
# FRONTEND INTEGRADO CON PESTAÑAS (B2B, CAOS/RED, AUDITORÍA)
# ==============================================================================

HTML_DASHBOARD = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SupplyGrid · Portal B2B y Consola de Resiliencia en Tiempo Real</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg-main: #0b1120;
    --bg-card: #1e293b;
    --bg-card-alt: #162032;
    --border: #334155;
    --border-light: #475569;
    --text-primary: #f8fafc;
    --text-secondary: #94a3b8;
    --accent-blue: #38bdf8;
    --accent-green: #22c55e;
    --accent-red: #ef4444;
    --accent-amber: #f59e0b;
    --accent-purple: #c084fc;
    --accent-cyan: #06b6d4;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
  body { background: var(--bg-main); color: var(--text-primary); padding: 16px 20px; font-size: 13px; }

  /* CABECERA PRINCIPAL */
  .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid var(--border); padding-bottom: 12px; margin-bottom: 14px; }
  .logo-group { display: flex; align-items: center; gap: 12px; }
  .logo-box { background: linear-gradient(135deg, #0284c7, #6366f1); color: white; font-weight: 900; font-size: 18px; padding: 6px 12px; border-radius: 8px; letter-spacing: 1px; }
  .header-titles h1 { font-size: 18px; font-weight: 800; color: var(--text-primary); }
  .header-titles p { font-size: 11px; color: var(--text-secondary); margin-top: 2px; }

  /* BOTÓN KILL SWITCH */
  .btn-kill { background: #b91c1c; color: white; font-size: 12px; font-weight: 800; padding: 8px 18px; border: none; border-radius: 6px; cursor: pointer; box-shadow: 0 0 16px rgba(239,68,68,0.4); transition: 0.2s; }
  .btn-kill:hover { background: #991b1b; transform: scale(1.02); }

  /* SISTEMA DE PESTAÑAS (TABS) */
  .tabs-nav { display: flex; gap: 8px; margin-bottom: 14px; border-bottom: 1px solid var(--border); padding-bottom: 8px; }
  .tab-btn { background: transparent; border: 1px solid var(--border); color: var(--text-secondary); padding: 8px 18px; border-radius: 6px; font-weight: 700; cursor: pointer; font-size: 12px; transition: 0.2s; }
  .tab-btn:hover { background: #1e293b; color: white; }
  .tab-btn.active { background: #0284c7; border-color: #38bdf8; color: white; box-shadow: 0 2px 8px rgba(2,132,199,0.3); }

  .tab-pane { display: none; }
  .tab-pane.active { display: block; }

  /* TARJETAS RESUMEN (KPIS) */
  .grid-kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px; margin-bottom: 14px; }
  .card-kpi { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 12px; text-align: center; }
  .card-kpi .label { font-size: 10px; text-transform: uppercase; color: var(--text-secondary); font-weight: 700; letter-spacing: 0.5px; }
  .card-kpi .value { font-size: 22px; font-weight: 800; margin: 3px 0; }
  .card-kpi .detail { font-size: 11px; color: var(--text-secondary); }

  /* BANNER DE ESTADO DEL NEGOCIO */
  .biz-status-banner { display: flex; justify-content: space-between; align-items: center; background: #064e3b; border: 1px solid #059669; padding: 10px 14px; border-radius: 8px; margin-bottom: 14px; }
  .biz-status-banner.alert { background: #7f1d1d; border-color: #dc2626; }
  .biz-status-banner.warning { background: #78350f; border-color: #d97706; }

  /* TABLAS DE NEGOCIO Y TRAZAS */
  .table-box { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 14px; margin-bottom: 14px; }
  .table-box h3 { font-size: 13px; font-weight: 700; color: var(--accent-blue); margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center; }
  table.data-table { width: 100%; border-collapse: collapse; font-size: 11px; }
  table.data-table th { background: #0f172a; padding: 8px 10px; text-align: left; color: var(--text-secondary); border-bottom: 1px solid var(--border); }
  table.data-table td { padding: 7px 10px; border-bottom: 1px solid #1e293b; }
  table.data-table tr:hover { background: #1e293b88; }

  .status-badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 10px; font-weight: 700; }
  .status-badge.ok { background: #065f46; color: #34d399; }
  .status-badge.err { background: #7f1d1d; color: #f87171; }
  .status-badge.warn { background: #78350f; color: #fbbf24; }

  /* BOTONES Y CONTROLES */
  .btn { cursor: pointer; border: none; border-radius: 6px; padding: 7px 13px; font-weight: 700; font-size: 11px; transition: 0.15s; }
  .btn-green { background: var(--accent-green); color: #022c22; }
  .btn-green:hover { background: #16a34a; }
  .btn-red { background: var(--accent-red); color: white; }
  .btn-red:hover { background: #dc2626; }
  .btn-amber { background: var(--accent-amber); color: #451a03; }
  .btn-amber:hover { background: #d97706; }
  .btn-blue { background: #0284c7; color: white; }
  .btn-blue:hover { background: #0369a1; }
  .btn-purple { background: #9333ea; color: white; }
  .btn-purple:hover { background: #7e22ce; }

  /* CAOS Y GRÁFICAS */
  .grid-charts { display: grid; grid-template-columns: 2fr 1fr; gap: 12px; margin-bottom: 14px; }
  .card-chart { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
  .card-chart h3 { font-size: 12px; color: var(--text-secondary); margin-bottom: 8px; text-transform: uppercase; }

  .control-panel { background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; padding: 14px; margin-bottom: 14px; }
  .control-panel h2 { font-size: 14px; color: var(--accent-blue); display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }

  .traffic-bar { display: flex; gap: 10px; align-items: center; background: #0f172a; padding: 8px 12px; border-radius: 6px; margin-bottom: 12px; flex-wrap: wrap; }
  .families-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 10px; }
  .family-card { background: #0f172a; border: 1px solid var(--border); border-radius: 6px; padding: 10px; }
  .family-card h4 { font-size: 11px; color: var(--accent-amber); margin-bottom: 6px; border-bottom: 1px solid var(--border); padding-bottom: 3px; }
  .family-card .btn-group { display: flex; gap: 6px; flex-wrap: wrap; }

  .code-tag { font-family: monospace; background: #0f172a; padding: 2px 6px; border-radius: 4px; font-size: 10px; color: #38bdf8; }
</style>
</head>
<body>

<!-- CABECERA -->
<div class="header">
  <div class="logo-group">
    <div class="logo-box">SG</div>
    <div class="header-titles">
      <h1>SUPPLYGRID · PLATAFORMA B2B & CONSOLA DE RESILIENCIA</h1>
      <p>Gestión de Pedidos, Proveedores y Arquitectura Reactiva ante Fallas · Semestre 7</p>
    </div>
  </div>
  <div style="display:flex; align-items:center; gap:12px;">
    <div id="badge-live-traffic" style="font-size: 11px; color: var(--accent-green); font-weight: bold; background: #022c22; border: 1px solid #16a34a; padding: 4px 10px; border-radius: 20px;">
      ● TRÁFICO BASELINE ACTIVO (40 ops/s)
    </div>
    <button class="btn btn-kill" onclick="triggerKillSwitch()">🚨 KILL SWITCH GLOBAL</button>
  </div>
</div>

<!-- NAVEGACIÓN POR PESTAÑAS -->
<div class="tabs-nav">
  <button class="tab-btn active" onclick="verPestana('pestana-portal-b2b')">📦 Portal B2B (Negocio)</button>
  <button class="tab-btn" onclick="verPestana('pestana-caos-metricas')">⚡ Tablero de Caos & Observabilidad (RED/USE)</button>
  <button class="tab-btn" onclick="verPestana('pestana-auditoria-trazas')">🛡️ Auditoría de Fallas & Trazas Causales (trace_id)</button>
</div>

<!-- ============================================================================== -->
<!-- PESTAÑA 1: PORTAL B2B (NEGOCIO)                                               -->
<!-- ============================================================================== -->
<div id="pestana-portal-b2b" class="tab-pane active">

  <!-- BANNER DE SALUD DEL NEGOCIO -->
  <div id="banner-negocio" class="biz-status-banner">
    <div>
      <span style="font-size:14px; font-weight:800;" id="banner-texto-titulo">🟢 OPERANDO CON NORMALIDAD</span>
      <div style="font-size:11px; opacity:0.9; margin-top:2px;" id="banner-texto-desc">
        Todos los módulos B2B (Proveedores, Catálogo, Órdenes, Logística) respondiendo dentro del SLA (&lt; 2 ms).
      </div>
    </div>
    <div>
      <button class="btn btn-blue" onclick="crearPedidoManual()">🛒 Simular Nueva Orden B2B</button>
    </div>
  </div>

  <!-- KPIS DEL NEGOCIO -->
  <div class="grid-kpis">
    <div class="card-kpi">
      <div class="label">Total Facturado B2B</div>
      <div class="value" id="kpi-facturado" style="color:var(--accent-green);">$ 1,450 M</div>
      <div class="detail">Transacciones confirmadas hoy</div>
    </div>
    <div class="card-kpi">
      <div class="label">Cumplimiento SLA</div>
      <div class="value" id="kpi-sla" style="color:var(--accent-blue);">99.4 %</div>
      <div class="detail">Compromiso contractual (&gt;98%)</div>
    </div>
    <div class="card-kpi">
      <div class="label">Proveedores Homologados</div>
      <div class="value" style="color:var(--accent-purple);">5 Aliados</div>
      <div class="detail">40% volumen en 2 líderes (Zipf)</div>
    </div>
    <div class="card-kpi">
      <div class="label">Tasa de Entrega a Tiempo</div>
      <div class="value" id="kpi-eta-ontime" style="color:var(--accent-cyan);">98.8 %</div>
      <div class="detail">Despachos logísticos en ETA</div>
    </div>
  </div>

  <!-- TABLA EN VIVO DE PEDIDOS B2B -->
  <div class="table-box">
    <h3>
      <span>Órdenes de Compra y Pedidos B2B en Tiempo Real</span>
      <span style="font-size: 11px; color: var(--text-secondary); font-weight: normal;">Flujo continuo de transacciones concurrentes</span>
    </h3>
    <table class="data-table">
      <thead>
        <tr>
          <th style="width: 90px;">Código Orden</th>
          <th style="width: 70px;">Hora</th>
          <th style="width: 220px;">Proveedor B2B</th>
          <th style="width: 200px;">Producto / SKU</th>
          <th style="width: 130px;">Total</th>
          <th style="width: 160px;">ETA / Entrega</th>
          <th style="width: 170px;">Estado de Negocio</th>
          <th style="width: 80px;">Latencia</th>
          <th>trace_id</th>
        </tr>
      </thead>
      <tbody id="tbody-pedidos">
        <tr><td colspan="9" style="text-align:center; color:var(--text-secondary);">Cargando flujo de pedidos B2B...</td></tr>
      </tbody>
    </table>
  </div>

  <!-- DIRECTORIO DE PROVEEDORES HOMOLOGADOS -->
  <div class="table-box">
    <h3>Directorio de Proveedores B2B Registrados (Regla Zipf: Top 2 concentran 40%)</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>ID</th>
          <th>Razón Social</th>
          <th>NIT</th>
          <th>Ciudad</th>
          <th>Calificación</th>
          <th>SLA Histórico</th>
          <th>Contrato Q1</th>
          <th>Latencia Media</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td><span class="code-tag">PRV-01</span></td>
          <td><strong>Logística & Cargas del Café S.A.S.</strong> ⭐ Líder Zipf</td>
          <td>900.123.456-1</td>
          <td>Medellín</td>
          <td style="color:var(--accent-amber);">★ 4.9 / 5.0</td>
          <td style="color:var(--accent-green); font-weight:bold;">99.2%</td>
          <td><span class="status-badge ok">VIGENTE</span></td>
          <td>0.4 ms</td>
        </tr>
        <tr>
          <td><span class="code-tag">PRV-02</span></td>
          <td><strong>BioFarma Insumos Industriales S.A.</strong> ⭐ Líder Zipf</td>
          <td>811.987.654-3</td>
          <td>Bogotá D.C.</td>
          <td style="color:var(--accent-amber);">★ 5.0 / 5.0</td>
          <td style="color:var(--accent-green); font-weight:bold;">98.8%</td>
          <td><span class="status-badge ok">VIGENTE</span></td>
          <td>0.6 ms</td>
        </tr>
        <tr>
          <td><span class="code-tag">PRV-03</span></td>
          <td>Empaques & Polímeros de Occidente</td>
          <td>890.321.789-5</td>
          <td>Cali</td>
          <td style="color:var(--accent-amber);">★ 4.7 / 5.0</td>
          <td style="color:var(--accent-green);">97.5%</td>
          <td><span class="status-badge ok">VIGENTE</span></td>
          <td>0.8 ms</td>
        </tr>
        <tr>
          <td><span class="code-tag">PRV-04</span></td>
          <td>Química Global del Caribe S.A.</td>
          <td>800.456.789-2</td>
          <td>Barranquilla</td>
          <td style="color:var(--accent-amber);">★ 4.8 / 5.0</td>
          <td style="color:var(--accent-green);">98.1%</td>
          <td><span class="status-badge ok">VIGENTE</span></td>
          <td>0.7 ms</td>
        </tr>
        <tr>
          <td><span class="code-tag">PRV-05</span></td>
          <td>TechSupply Hardware B2B S.A.S.</td>
          <td>901.654.321-8</td>
          <td>Manizales</td>
          <td style="color:var(--accent-amber);">★ 4.9 / 5.0</td>
          <td style="color:var(--accent-green); font-weight:bold;">99.4%</td>
          <td><span class="status-badge ok">VIGENTE</span></td>
          <td>0.5 ms</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>

<!-- ============================================================================== -->
<!-- PESTAÑA 2: TABLERO DE CAOS & OBSERVABILIDAD (RED / USE)                       -->
<!-- ============================================================================== -->
<div id="pestana-caos-metricas" class="tab-pane">

  <!-- TARJETAS DE MÉTRICAS RED Y USE -->
  <div class="grid-kpis">
    <div class="card-kpi">
      <div class="label">Latencia p99 (Cola)</div>
      <div class="value" id="val-p99" style="color: var(--accent-amber);">0.0 ms</div>
      <div class="detail">p50: <span id="val-p50">0.0</span> | p95: <span id="val-p95">0.0</span></div>
    </div>
    <div class="card-kpi">
      <div class="label">Throughput (Rate)</div>
      <div class="value" id="val-tput" style="color: var(--accent-blue);">0</div>
      <div class="detail">operaciones / seg</div>
    </div>
    <div class="card-kpi">
      <div class="label">Tasa de Errores</div>
      <div class="value" id="val-err" style="color: var(--accent-green);">0.0 %</div>
      <div class="detail">Total errores: <span id="val-err-count">0</span></div>
    </div>
    <div class="card-kpi">
      <div class="label">Utilización CPU</div>
      <div class="value" id="val-cpu" style="color: var(--accent-purple);">0.0 %</div>
      <div class="detail">RAM RSS: <span id="val-rss">0.0</span> MB</div>
    </div>
    <div class="card-kpi">
      <div class="label">Estado del Caos</div>
      <div class="value" id="val-chaos-status" style="font-size: 15px; margin: 6px 0; color: var(--accent-green);">NORMAL</div>
      <div class="detail" id="val-fault-name">Sin fallas activas</div>
    </div>
  </div>

  <!-- GRÁFICOS EN TIEMPO REAL -->
  <div class="grid-charts">
    <div class="card-chart">
      <h3>Latencia en Tiempo Real: p50 / p95 / p99 (Milisegundos)</h3>
      <div style="height: 200px;"><canvas id="chartLatency"></canvas></div>
    </div>
    <div class="card-chart">
      <h3>Throughput y Tasa de Errores</h3>
      <div style="height: 200px;"><canvas id="chartTputErr"></canvas></div>
    </div>
  </div>

  <!-- PANEL DE CONTROL DE FALLAS Y CARGA -->
  <div class="control-panel">
    <h2>
      <span>Control de Inyección de Fallas y Mitigaciones en Caliente</span>
      <button class="btn btn-amber" onclick="resetCatalogo()">Restablecer Todo (Reset)</button>
    </h2>

    <div class="traffic-bar">
      <span style="font-size: 11px; font-weight: bold; color: var(--accent-blue);">TRÁFICO CONCURRENTE:</span>
      <button class="btn btn-green" id="btn-traffic-toggle" onclick="toggleTrafico()">⏸ PAUSAR TRÁFICO</button>
      
      <label style="font-size: 11px; color: var(--text-secondary); margin-left: 8px;">Tasa:</label>
      <select id="sel-rps" onchange="actualizarTrafico()" style="background:#1e293b; color:white; border:1px solid #334155; padding:4px 6px; border-radius:4px;">
        <option value="15">15 rps (Ligera)</option>
        <option value="40" selected>40 rps (Normal Baseline)</option>
        <option value="80">80 rps (Alta)</option>
        <option value="150">150 rps (Estrés)</option>
      </select>

      <label style="font-size: 11px; color: var(--text-secondary); margin-left: 12px;">ESTRATEGIA MITIGACIÓN:</label>
      <select id="sel-mitigacion" onchange="actualizarTrafico()" style="background:#1e293b; color:white; border:1px solid #334155; padding:4px 6px; border-radius:4px;">
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
          <button class="btn btn-amber" onclick="activarFalla('A1_latencia_jitter', {mean_ms: 35.0, jitter_ms: 8.0}, 1.0)">A1: Latencia 35ms</button>
          <button class="btn btn-amber" onclick="activarFalla('A2_timeout_controlado', {timeout_ms: 12.0}, 1.0)">A2: Timeout 12ms</button>
        </div>
      </div>

      <!-- FAMILIA B -->
      <div class="family-card">
        <h4>FAMILIA B: Falla de Base de Datos</h4>
        <div class="btn-group">
          <button class="btn btn-red" onclick="activarFalla('B1_pool_agotado', {rejection_probability: 0.45}, 0.45)">B1: Pool Agotado (45%)</button>
          <button class="btn btn-red" onclick="activarFalla('B2_bloqueo_recurso', {lock_wait_ms: 90.0, target_key: 'proveedor_1'}, 1.0)">B2: Bloqueo Recurso</button>
        </div>
      </div>

      <!-- FAMILIA C -->
      <div class="family-card">
        <h4>FAMILIA C: Degradación de Recursos</h4>
        <div class="btn-group">
          <button class="btn btn-purple" onclick="activarFalla('C1_saturacion_cpu', {intensity_pct: 70.0, duration_s: 15.0, threads: 2}, 1.0)">C1: CPU 70% (15s auto)</button>
          <button class="btn btn-purple" onclick="activarFalla('C2_presion_memoria', {size_mb: 250.0, duration_s: 15.0}, 1.0)">C2: RAM 250MB (15s)</button>
        </div>
      </div>

      <!-- FAMILIA D -->
      <div class="family-card">
        <h4>FAMILIA D: Disponibilidad</h4>
        <div class="btn-group">
          <button class="btn btn-blue" onclick="activarFalla('D2_respuestas_5xx', {status_code: 503}, 0.35)">D2: Error 503 (35%)</button>
          <button class="btn btn-blue" onclick="activarFalla('D1_particion_red', {duration_s: 5.0}, 1.0)">D1: Corte Red (5s auto)</button>
        </div>
      </div>
    </div>
  </div>
</div>

<!-- ============================================================================== -->
<!-- PESTAÑA 3: AUDITORÍA DE FALLAS & TRAZAS CAUSALES (trace_id)                   -->
<!-- ============================================================================== -->
<div id="pestana-auditoria-trazas" class="tab-pane">
  <div class="table-box">
    <h3>
      <span>Stream Causal de Trazas e Intercepciones (trace_id)</span>
      <div style="display:flex; gap:8px;">
        <label style="font-size:11px; font-weight:normal; color:var(--text-secondary);">
          <input type="checkbox" id="chk-solo-errores" onchange="renderTrazas()"> Solo Errores
        </label>
      </div>
    </h3>
    <table class="data-table">
      <thead>
        <tr>
          <th style="width: 70px;">Hora</th>
          <th style="width: 170px;">Target Interceptado</th>
          <th style="width: 80px;">Resultado</th>
          <th style="width: 90px;">Duración</th>
          <th style="width: 320px;">Detalle / Excepción Capturada</th>
          <th>trace_id de Correlación Causal</th>
        </tr>
      </thead>
      <tbody id="trace-tbody">
        <tr><td colspan="6" style="text-align: center; color: var(--text-secondary);">Cargando trazas del sistema...</td></tr>
      </tbody>
    </table>
  </div>
</div>

<script>
let pestanaActual = 'pestana-portal-b2b';
let traficoActivo = true; // Arranca activo por defecto con la baseline
let ultimasTrazasCache = [];

function verPestana(id) {
  document.querySelectorAll('.tab-pane').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  event.target.classList.add('active');
  pestanaActual = id;
}

// Configuración de Chart.js
const ctxLat = document.getElementById('chartLatency').getContext('2d');
const chartLat = new Chart(ctxLat, {
  type: 'line',
  data: {
    labels: [],
    datasets: [
      { label: 'p99 (ms)', data: [], borderColor: '#f59e0b', backgroundColor: 'transparent', tension: 0.2, borderWidth: 2, pointRadius: 1 },
      { label: 'p95 (ms)', data: [], borderColor: '#38bdf8', backgroundColor: 'transparent', tension: 0.2, borderWidth: 1.5, pointRadius: 0 },
      { label: 'p50 (ms)', data: [], borderColor: '#22c55e', backgroundColor: 'transparent', tension: 0.2, borderWidth: 1.5, pointRadius: 0 },
    ]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    scales: {
      x: { ticks: { color: '#64748b', maxTicksLimit: 8 }, grid: { color: '#1e293b' } },
      y: { ticks: { color: '#64748b' }, grid: { color: '#1e293b' }, beginAtZero: true }
    },
    plugins: { legend: { labels: { color: '#cbd5e1', boxWidth: 10, font: { size: 10 } } } }
  }
});

const ctxTput = document.getElementById('chartTputErr').getContext('2d');
const chartTput = new Chart(ctxTput, {
  type: 'line',
  data: {
    labels: [],
    datasets: [
      { label: 'Throughput (ops/s)', data: [], borderColor: '#38bdf8', backgroundColor: 'transparent', yAxisID: 'y', tension: 0.2, borderWidth: 2, pointRadius: 0 },
      { label: 'Errores (%)', data: [], borderColor: '#ef4444', backgroundColor: 'transparent', yAxisID: 'y1', tension: 0.2, borderWidth: 2, pointRadius: 0 },
    ]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    scales: {
      x: { ticks: { color: '#64748b', maxTicksLimit: 6 }, grid: { color: '#1e293b' } },
      y: { type: 'linear', position: 'left', ticks: { color: '#38bdf8' }, grid: { color: '#1e293b' }, beginAtZero: true },
      y1: { type: 'linear', position: 'right', min: 0, max: 100, ticks: { color: '#ef4444' }, grid: { display: false } }
    },
    plugins: { legend: { labels: { color: '#cbd5e1', boxWidth: 10, font: { size: 10 } } } }
  }
});

async function actualizarDashboard() {
  try {
    const res = await fetch('/api/estado');
    const data = await res.json();

    const m = data.metricas;
    const c = data.caos;
    const hist = data.historico;

    // Métricas numéricas
    document.getElementById('val-p99').innerText = m.p99.toFixed(1) + ' ms';
    document.getElementById('val-p50').innerText = m.p50.toFixed(1);
    document.getElementById('val-p95').innerText = m.p95.toFixed(1);
    document.getElementById('val-tput').innerText = m.throughput;
    document.getElementById('val-err').innerText = m.err_pct.toFixed(1) + ' %';
    document.getElementById('val-err-count').innerText = m.total_errores;
    document.getElementById('val-cpu').innerText = m.cpu_pct.toFixed(1) + ' %';
    document.getElementById('val-rss').innerText = m.rss_mb.toFixed(1);

    // Estado del Caos y Banner B2B
    const statusEl = document.getElementById('val-chaos-status');
    const faultEl = document.getElementById('val-fault-name');
    const banner = document.getElementById('banner-negocio');
    const bannerTit = document.getElementById('banner-texto-titulo');
    const bannerDesc = document.getElementById('banner-texto-desc');
    const kpiSla = document.getElementById('kpi-sla');

    if (c.kill_switch) {
      statusEl.innerText = '🚨 KILL SWITCH ACTIVO';
      statusEl.style.color = 'var(--accent-red)';
      faultEl.innerText = 'Todas las perturbaciones abortadas';
      banner.className = 'biz-status-banner alert';
      bannerTit.innerText = '🚨 EMERGENCIA: SISTEMA EN MODO SEGURO (KILL SWITCH)';
      bannerDesc.innerText = 'Se forzó la parada total del motor de caos. Tráfico reencaminado limpiamente.';
    } else if (c.fallas_activas && c.fallas_activas.length > 0) {
      const fName = c.fallas_activas.join(', ');
      statusEl.innerText = '⚡ FALLA INYECTADA';
      statusEl.style.color = 'var(--accent-amber)';
      faultEl.innerText = fName;

      banner.className = 'biz-status-banner alert';
      bannerTit.innerText = '⚠️ DEGRADACIÓN DETECTADA: ' + fName;
      bannerDesc.innerText = 'La inyección de fallas está afectando la resiliencia. Observa cómo responden las métricas.';
    } else {
      statusEl.innerText = 'NORMAL';
      statusEl.style.color = 'var(--accent-green)';
      faultEl.innerText = 'Sin fallas activas';

      banner.className = 'biz-status-banner';
      bannerTit.innerText = '🟢 OPERANDO CON NORMALIDAD';
      bannerDesc.innerText = 'Todos los módulos B2B (Proveedores, Catálogo, Órdenes, Logística) dentro del SLA (< 2 ms).';
    }

    // Calcular SLA en vivo
    const slaActual = Math.max(0, 100.0 - m.err_pct).toFixed(1);
    kpiSla.innerText = slaActual + ' %';
    kpiSla.style.color = (slaActual >= 98.0) ? 'var(--accent-green)' : ((slaActual >= 80.0) ? 'var(--accent-amber)' : 'var(--accent-red)');

    // KPI Facturación
    if (data.negocio && data.negocio.facturado_cop) {
      const mills = (data.negocio.facturado_cop / 1000000).toFixed(1);
      document.getElementById('kpi-facturado').innerText = `$ ${mills} M`;
    }

    // Actualizar Gráficas
    if (hist.timestamps && hist.timestamps.length > 0) {
      chartLat.data.labels = hist.timestamps;
      chartLat.data.datasets[0].data = hist.p99;
      chartLat.data.datasets[1].data = hist.p95;
      chartLat.data.datasets[2].data = hist.p50;
      chartLat.update();

      chartTput.data.labels = hist.timestamps;
      chartTput.data.datasets[0].data = hist.throughput;
      chartTput.data.datasets[1].data = hist.err_pct;
      chartTput.update();
    }

    // Tabla de Pedidos B2B
    if (data.pedidos && data.pedidos.length > 0) {
      renderPedidosB2B(data.pedidos);
    }

    // Trazas
    ultimasTrazasCache = data.trazas || [];
    renderTrazas();

  } catch (err) {
    console.error('Error al actualizar dashboard:', err);
  }
}

function renderPedidosB2B(pedidos) {
  const tbody = document.getElementById('tbody-pedidos');
  if (!tbody) return;
  let html = '';
  for (const p of pedidos) {
    const badgeClass = p.exito ? 'ok' : 'err';
    html += `
      <tr>
        <td><strong class="code-tag">${p.id}</strong></td>
        <td>${p.hora}</td>
        <td>${p.proveedor}</td>
        <td>${p.producto}</td>
        <td style="font-weight:bold;">${p.monto_fmt}</td>
        <td><span style="color:var(--accent-cyan); font-size:10px;">🕒 ${p.eta}</span></td>
        <td><span class="status-badge ${badgeClass}">${p.estado}</span></td>
        <td>${p.dur_ms} ms</td>
        <td><span style="font-family:monospace; color:#64748b; font-size:10px;">${p.trace_id}</span></td>
      </tr>
    `;
  }
  tbody.innerHTML = html;
}

function renderTrazas() {
  const tbody = document.getElementById('trace-tbody');
  if (!tbody) return;
  const soloErrores = document.getElementById('chk-solo-errores').checked;
  const lista = soloErrores ? ultimasTrazasCache.filter(t => t.resultado === 'error') : ultimasTrazasCache;

  if (lista.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: #64748b;">No hay eventos registrados con el filtro actual.</td></tr>';
    return;
  }

  let html = '';
  for (const t of lista) {
    const resClass = t.resultado === 'ok' ? 'status-badge ok' : 'status-badge err';
    html += `
      <tr>
        <td>${t.ts}</td>
        <td><span class="code-tag">${t.target}</span></td>
        <td><span class="${resClass}">${t.resultado.toUpperCase()}</span></td>
        <td>${t.dur_ms} ms</td>
        <td style="color:${t.error ? 'var(--accent-red)' : 'var(--text-secondary)'}; font-size:10px;">${t.error || 'Operación completada exitosamente'}</td>
        <td><span style="font-family:monospace; color:#64748b; font-size:10px;">${t.trace_id}</span></td>
      </tr>
    `;
  }
  tbody.innerHTML = html;
}

async function activarFalla(faultId, params, trafficPct) {
  try {
    const res = await fetch('/api/falla/activar', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        fault_id: faultId,
        parameters: params,
        traffic_pct: trafficPct !== undefined ? trafficPct : 1.0
      })
    });
    const data = await res.json();
    actualizarDashboard();
  } catch (err) {
    alert('Error al activar falla: ' + err);
  }
}

async function triggerKillSwitch() {
  try {
    const res = await fetch('/api/kill-switch', { method: 'POST' });
    const data = await res.json();
    actualizarDashboard();
  } catch (err) {
    alert('Error al accionar Kill Switch: ' + err);
  }
}

async function resetCatalogo() {
  try {
    const res = await fetch('/api/reset', { method: 'POST' });
    actualizarDashboard();
  } catch (err) {
    alert('Error al restablecer catálogo: ' + err);
  }
}

async function toggleTrafico() {
  traficoActivo = !traficoActivo;
  const rps = parseInt(document.getElementById('sel-rps').value);
  const mitigacion = document.getElementById('sel-mitigacion').value;
  await fetch('/api/trafico', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ activo: traficoActivo, rps: rps, mitigacion: mitigacion })
  });

  const btn = document.getElementById('btn-traffic-toggle');
  const badge = document.getElementById('badge-live-traffic');
  if (traficoActivo) {
    btn.className = 'btn btn-green';
    btn.innerText = '⏸ PAUSAR TRÁFICO';
    badge.innerText = `● TRÁFICO BASELINE ACTIVO (${rps} ops/s)`;
    badge.style.color = 'var(--accent-green)';
  } else {
    btn.className = 'btn btn-amber';
    btn.innerText = '▶ REANUDAR TRÁFICO';
    badge.innerText = '○ TRÁFICO PAUSADO';
    badge.style.color = 'var(--text-secondary)';
  }
}

async function actualizarTrafico() {
  if (!traficoActivo) return;
  const rps = parseInt(document.getElementById('sel-rps').value);
  const mitigacion = document.getElementById('sel-mitigacion').value;
  await fetch('/api/trafico', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ activo: true, rps: rps, mitigacion: mitigacion })
  });
  document.getElementById('badge-live-traffic').innerText = `● TRÁFICO BASELINE ACTIVO (${rps} ops/s)`;
}

async function crearPedidoManual() {
  try {
    const res = await fetch('/api/pedido/crear', { method: 'POST' });
    const data = await res.json();
    actualizarDashboard();
  } catch (err) {
    console.error('Error al crear pedido manual:', err);
  }
}

setInterval(actualizarDashboard, 1000);
actualizarDashboard();
</script>

</body>
</html>
"""


# ==============================================================================
# MANEJADOR HTTP REST
# ==============================================================================

class TableroHttpHandler(http.server.BaseHTTPRequestHandler):
    manager: ChaosConfigManager
    generador: GeneradorTrafico

    def log_message(self, format, *args):
        pass

    def _responder_json(self, codigo: int, data: Dict[str, Any]):
        body = json.dumps(data).encode("utf-8")
        self.send_response(codigo)
        self.send_header("Content-Type", "application/json")
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
                        "rss_mb": telemetria.historico_rss[-1] if telemetria.historico_rss else 0.0,
                    },
                    "caos": {
                        "kill_switch": cfg.kill_switch,
                        "enabled": cfg.enabled,
                        "fallas_activas": fallas_on
                    },
                    "negocio": {
                        "facturado_cop": telemetria.total_facturado_cop,
                    },
                    "historico": {
                        "timestamps": list(telemetria.historico_timestamps),
                        "p50": list(telemetria.historico_p50),
                        "p95": list(telemetria.historico_p95),
                        "p99": list(telemetria.historico_p99),
                        "throughput": list(telemetria.historico_tput),
                        "err_pct": list(telemetria.historico_err_pct),
                    },
                    "pedidos": list(telemetria.ultimos_pedidos),
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
            if nuevo_estado:
                self.generador.engine.stop_all_disturbances()
            obs.kill_switch("activado_desde_tablero" if nuevo_estado else "desactivado")
            self._responder_json(200, {"ok": True, "kill_switch": nuevo_estado})
            return

        if parsed.path == "/api/falla/activar":
            fault_id = body.get("fault_id")
            params = body.get("parameters", {})
            traffic_pct = body.get("traffic_pct")
            if traffic_pct is None:
                traffic_pct = params.get("traffic_pct", params.get("rejection_probability", 1.0))
            traffic_pct = float(traffic_pct)

            if not self.manager.get_config().enabled:
                self.manager.set_global_enabled(True)
            self.manager.set_fault_state(fault_id, True, parameters=params, traffic_pct=traffic_pct)
            obs.falla(fault_id, True)

            falla_obj = self.manager.get_config().faults.get(fault_id)
            if falla_obj:
                # C1: Quema de CPU multihilo con auto-recuperación
                if fault_id == "C1_saturacion_cpu":
                    self.generador.engine.start_cpu_burn(falla_obj)
                    dur_s = float(params.get("duration_s", 15.0))
                    def _auto_apagar_cpu():
                        time.sleep(dur_s)
                        self.manager.set_fault_state(fault_id, False)
                    threading.Thread(target=_auto_apagar_cpu, daemon=True).start()

                # C2: Presión de memoria RAM con auto-recuperación
                elif fault_id == "C2_presion_memoria":
                    self.generador.engine.apply_memory_pressure(falla_obj)
                    dur_s = float(params.get("duration_s", 15.0))
                    def _auto_apagar_ram():
                        time.sleep(dur_s)
                        self.manager.set_fault_state(fault_id, False)
                    threading.Thread(target=_auto_apagar_ram, daemon=True).start()

                # D1: Corte de Red transitorio con auto-recuperación exacta (5 segundos)
                elif fault_id == "D1_particion_red":
                    dur_s = float(params.get("duration_s", 5.0))
                    def _auto_recuperar_red():
                        time.sleep(dur_s)
                        self.manager.set_fault_state(fault_id, False)
                        obs.evento("recuperacion_automatica_red", fault=fault_id, duracion_s=dur_s)
                    threading.Thread(target=_auto_recuperar_red, daemon=True).start()

            self._responder_json(200, {"ok": True, "falla": fault_id, "activa": True, "traffic_pct": traffic_pct})
            return

        if parsed.path == "/api/reset":
            self.generador.engine.stop_all_disturbances()
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

        if parsed.path == "/api/pedido/crear":
            # Pedido manual bajo demanda desde el botón de la interfaz B2B
            datos_pedido = self.generador._generar_pedido_aleatorio()
            self.generador.ejecutar_transaccion("Q3_ordenes_compra", datos_pedido)
            self._responder_json(200, {"ok": True, "pedido": datos_pedido})
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

    # Iniciar automáticamente el tráfico concurrente baseline (40 rps)
    generador.iniciar(rps=40, mitigacion="ninguna")

    server = socketserver.TCPServer(("0.0.0.0", puerto), TableroHttpHandler)

    url = f"http://localhost:{puerto}"
    print("=" * 75)
    print(" SUPPLYGRID · PLATAFORMA B2B Y CONSOLA DE RESILIENCIA EN TIEMPO REAL")
    print("=" * 75)
    print(f" Servidor activo en        : {url}")
    print(f" Tráfico baseline          : ACTIVO por defecto (40 rps)")
    print(f" Archivo de configuración  : {config_path}")
    print(" Presiona Ctrl+C para detener el servidor.")
    print("=" * 75)

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
