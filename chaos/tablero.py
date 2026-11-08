"""
SupplyGrid Enterprise · Plataforma B2B y Consola de Resiliencia SRE en Tiempo Real.

Módulos de Negocio Integrados:
  - Portal de Operaciones B2B (Órdenes de Compra, Directorio de Proveedores, SLAs, KPIs).
  - Consola de Resiliencia y Caos (RED metrics, USE metrics, Inyección de 4 familias).
  - Auditoría Causal de Trazas (Correlación trace_id, Bitácora de Excepciones).
  - Tráfico Baseline Automático y Escalabilidad Multinúcleo Concurrente.

Uso:
    python -m chaos.tablero
    python tablero.py
"""

from __future__ import annotations

import collections
import hashlib
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

import psutil

from chaos.config import ChaosConfigManager
from chaos.engine import ChaosEngine, ChaosDatabaseError, ChaosHttpError, ChaosTimeoutError
from chaos import observabilidad as obs
from chaos.interceptor import with_chaos
from experimentos.mitigaciones import CacheTTL, CircuitBreaker, CircuitoAbierto

PUERTO_DEFECTO = int(os.environ.get("TABLERO_PORT", "8050"))
ROOT = Path(__file__).resolve().parent.parent

# ==============================================================================
# CATÁLOGO DE NEGOCIO B2B (Módulos Proveedores, Catálogo, Órdenes, Logística)
# ==============================================================================

PROVEEDORES_B2B = [
    {
        "id": 1,
        "codigo": "PRV-001",
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
        "codigo": "PRV-002",
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
        "codigo": "PRV-003",
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
        "codigo": "PRV-004",
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
        "codigo": "PRV-005",
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
    {"sku": "SKU-BIO-042", "nombre": "Pallet Insumo Químico Grado A", "precio": 12500000, "eta": "18h (Ruta Expreso)"},
    {"sku": "SKU-LOG-119", "nombre": "Contenedor Material Termorregulable", "precio": 28400000, "eta": "36h (Consolidación)"},
    {"sku": "SKU-EMP-883", "nombre": "Bobinas Polietileno Biodegradable (x50)", "precio": 8900000, "eta": "24h (Ruta Directa)"},
    {"sku": "SKU-TEC-501", "nombre": "Sensores IoT Cadena de Frío B2B", "precio": 16200000, "eta": "12h (Despacho Aéreo)"},
    {"sku": "SKU-QUI-772", "nombre": "Reactivo Analítico Certificado ISO", "precio": 34100000, "eta": "48h (Transporte Especial)"}
]


# ==============================================================================
# TELEMETRÍA GLOBAL Y ESTADO B2B EN TIEMPO REAL
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

    def registrar_transaccion(
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

            # Traza causal (Persona B)
            self.ultimas_trazas.appendleft({
                "ts": ahora_str,
                "target": target,
                "resultado": "ok" if exito else "error",
                "dur_ms": round(dur_ms, 2),
                "error": error_msg or None,
                "trace_id": trace_id or "local"
            })

            # Actualización de flujo B2B
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
                        estado_negocio = "CIRCUITO_ABIERTO (Fail-Fast)"
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
# GENERADOR DE TRÁFICO MULTIHILO (Carga de CPU real y proporcional)
# ==============================================================================

class GeneradorTrafico:
    def __init__(self, manager: ChaosConfigManager):
        self.manager = manager
        self.engine = ChaosEngine(manager)
        self.activo = False
        self.hilos: List[threading.Thread] = []
        self.rps = 40
        self.mitigacion = "ninguna"
        self.cache_ttl = CacheTTL(ttl_segundos=60, capacidad=500, nombre="Q1")
        self.breaker = CircuitBreaker(umbral_fallos=4, timeout_abierto_s=3.0, nombre="Q1")
        self.num_workers = 3

    def iniciar(self, rps: int = 40, mitigacion: str = "ninguna"):
        self.rps = rps
        self.mitigacion = mitigacion
        if self.activo:
            return
        self.activo = True
        self.hilos.clear()
        # Varios workers en paralelo para distribuir y escalar la CPU multihilo
        for i in range(self.num_workers):
            t = threading.Thread(target=self._worker_trafico, args=(i,), daemon=True)
            self.hilos.append(t)
            t.start()

    def detener(self):
        self.activo = False

    def _generar_pedido_aleatorio(self) -> Dict[str, Any]:
        # Ley de Zipf: el 40% de órdenes se concentra en los 2 proveedores principales
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
        """
        Simula la carga computacional real de una transacción B2B:
        Firma criptográfica de payload, serialización JSON y verificación de contrato.
        Esto hace que la CPU escale proporcionalmente al Throughput.
        """
        raw = f"order_{target}_{random.randint(100, 999)}_{time.perf_counter()}".encode("utf-8")
        # Trabajo computacional controlado por transacción (~0.35ms de CPU activo)
        for _ in range(450):
            raw = hashlib.md5(raw).digest()
        time.sleep(random.uniform(0.0001, 0.0003))
        return f"{target}_PROCESSED"

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
                    telemetria.registrar_transaccion(dur_ms, True, target, "", trace_id, datos_pedido)
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
                    telemetria.registrar_transaccion(
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
        telemetria.registrar_transaccion(dur_ms, exito, target, error_msg, trace_id, datos_pedido)

    def _worker_trafico(self, worker_id: int):
        targets = [
            "Q1_contratos_proveedor",
            "Q2_catalogo_sku",
            "Q3_ordenes_compra",
            "Q5_despacho_logistica",
            "ext_facturacion_tax"
        ]
        while self.activo:
            rps_por_worker = max(1, self.rps // self.num_workers)
            intervalo = 1.0 / rps_por_worker
            t_inicio = time.time()

            target = random.choice(targets)
            datos_pedido = self._generar_pedido_aleatorio() if random.random() < 0.20 else None

            self.ejecutar_transaccion(target, datos_pedido)

            espera = intervalo - (time.time() - t_inicio)
            if espera > 0:
                time.sleep(espera)


# ==============================================================================
# HILO RECOLECTOR DE TELEMETRÍA USE (CPU normalizada y RAM RSS)
# ==============================================================================

def bucle_telemetria_fondo():
    proc = psutil.Process()
    num_cores = psutil.cpu_count() or 1
    proc.cpu_percent(interval=None)
    while True:
        time.sleep(1.0)
        try:
            # En Windows con 16 cores, psutil reporta la fracción total sobre el sistema.
            # Multiplicamos para reflejar el impacto de utilización relativo a los núcleos asignados al proceso.
            raw_cpu = proc.cpu_percent(interval=None)
            rss_mb = proc.memory_info().rss / (1024 * 1024)
        except Exception:
            raw_cpu = 0.0
            rss_mb = 0.0

        telemetria.tick_segundo(raw_cpu, rss_mb)


# ==============================================================================
# FRONTEND EMPRESARIAL B2B (Sin emojis, paleta corporativa oscura, navegación clara)
# ==============================================================================

HTML_DASHBOARD = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SupplyGrid · Enterprise B2B Supply Chain & SRE Platform</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg-base: #090d16;
    --bg-surface: #111827;
    --bg-surface-elevated: #162032;
    --bg-surface-hover: #1e293b;
    --border-subtle: #1f293d;
    --border-strong: #334155;
    --text-main: #f1f5f9;
    --text-muted: #94a3b8;
    --text-subtle: #64748b;
    --brand-indigo: #4f46e5;
    --brand-indigo-hover: #4338ca;
    --brand-cyan: #0284c7;
    --status-ok: #10b981;
    --status-err: #ef4444;
    --status-warn: #f59e0b;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; }
  body { background: var(--bg-base); color: var(--text-main); font-size: 13px; line-height: 1.5; }

  /* CABECERA CORPORATIVA B2B */
  .enterprise-navbar {
    background: #0c1220;
    border-bottom: 1px solid var(--border-subtle);
    padding: 0 24px;
    height: 56px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .brand-group { display: flex; align-items: center; gap: 16px; }
  .brand-logo {
    background: var(--brand-indigo);
    color: #ffffff;
    font-weight: 900;
    font-size: 13px;
    letter-spacing: 1.5px;
    padding: 5px 10px;
    border-radius: 4px;
    text-transform: uppercase;
  }
  .brand-name { font-size: 15px; font-weight: 700; color: #ffffff; letter-spacing: -0.2px; }
  .brand-tag { font-size: 11px; color: var(--text-subtle); padding-left: 8px; border-left: 1px solid var(--border-strong); }

  /* LINKS DE NAVEGACIÓN PRINCIPAL */
  .nav-links { display: flex; gap: 4px; height: 100%; align-items: center; margin-left: 32px; }
  .nav-tab-btn {
    background: transparent;
    border: none;
    color: var(--text-muted);
    font-size: 12px;
    font-weight: 600;
    padding: 18px 14px;
    cursor: pointer;
    border-bottom: 2px solid transparent;
    transition: all 0.15s ease;
  }
  .nav-tab-btn:hover { color: #ffffff; }
  .nav-tab-btn.active {
    color: #ffffff;
    border-bottom-color: var(--brand-indigo);
    background: #11182740;
  }

  /* ACCIONES GLOBALES SUPERIORES */
  .nav-actions { display: flex; align-items: center; gap: 14px; }
  .status-pill {
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.5px;
    padding: 4px 10px;
    border-radius: 4px;
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .status-pill.normal { background: #064e3b33; color: var(--status-ok); border: 1px solid #05966955; }
  .status-pill.degraded { background: #78350f33; color: var(--status-warn); border: 1px solid #d9770655; }
  .status-pill.critical { background: #7f1d1d33; color: var(--status-err); border: 1px solid #dc262655; }

  .status-dot { width: 6px; height: 6px; border-radius: 50%; background: currentColor; }

  .btn-kill-switch {
    background: transparent;
    color: var(--status-err);
    border: 1px solid var(--status-err);
    font-size: 11px;
    font-weight: 800;
    letter-spacing: 0.5px;
    padding: 6px 14px;
    border-radius: 4px;
    cursor: pointer;
    transition: 0.15s;
    text-transform: uppercase;
  }
  .btn-kill-switch:hover { background: var(--status-err); color: #ffffff; }

  /* CONTENEDOR PRINCIPAL */
  .main-content { padding: 20px 24px; max-width: 1440px; margin: 0 auto; }
  .tab-pane { display: none; }
  .tab-pane.active { display: block; }

  /* TARJETAS RESUMEN (METRICAS / KPIS) */
  .grid-summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin-bottom: 20px; }
  .card-summary {
    background: var(--bg-surface);
    border: 1px solid var(--border-subtle);
    border-radius: 6px;
    padding: 14px 16px;
  }
  .card-summary .title { font-size: 10px; text-transform: uppercase; font-weight: 700; letter-spacing: 0.8px; color: var(--text-subtle); margin-bottom: 6px; }
  .card-summary .val { font-size: 24px; font-weight: 800; color: #ffffff; font-variant-numeric: tabular-nums; }
  .card-summary .subtitle { font-size: 11px; color: var(--text-muted); margin-top: 4px; }

  /* BANNER OPERACIONAL */
  .banner-operational {
    background: var(--bg-surface);
    border-left: 3px solid var(--brand-indigo);
    border-top: 1px solid var(--border-subtle);
    border-right: 1px solid var(--border-subtle);
    border-bottom: 1px solid var(--border-subtle);
    border-radius: 6px;
    padding: 12px 18px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 20px;
  }
  .banner-operational.warning { border-left-color: var(--status-warn); }
  .banner-operational.error { border-left-color: var(--status-err); }

  /* BOTONES CORPORATIVOS */
  .btn-primary {
    background: var(--brand-indigo);
    color: #ffffff;
    font-size: 12px;
    font-weight: 700;
    padding: 8px 16px;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    transition: 0.15s;
  }
  .btn-primary:hover { background: var(--brand-indigo-hover); }

  .btn-outline {
    background: transparent;
    color: var(--text-main);
    border: 1px solid var(--border-strong);
    font-size: 11px;
    font-weight: 600;
    padding: 6px 12px;
    border-radius: 4px;
    cursor: pointer;
    transition: 0.15s;
  }
  .btn-outline:hover { background: var(--bg-surface-hover); }

  /* TABLAS DE DATOS */
  .panel-box {
    background: var(--bg-surface);
    border: 1px solid var(--border-subtle);
    border-radius: 6px;
    margin-bottom: 20px;
    overflow: hidden;
  }
  .panel-header {
    padding: 12px 18px;
    border-bottom: 1px solid var(--border-subtle);
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: var(--bg-surface-elevated);
  }
  .panel-header h3 { font-size: 12px; text-transform: uppercase; letter-spacing: 0.6px; font-weight: 700; color: var(--text-main); }

  table.enterprise-table { width: 100%; border-collapse: collapse; font-size: 12px; }
  table.enterprise-table th {
    background: #0d1424;
    padding: 9px 14px;
    text-align: left;
    font-size: 10px;
    text-transform: uppercase;
    font-weight: 700;
    letter-spacing: 0.6px;
    color: var(--text-subtle);
    border-bottom: 1px solid var(--border-subtle);
  }
  table.enterprise-table td {
    padding: 10px 14px;
    border-bottom: 1px solid var(--border-subtle);
    color: var(--text-main);
  }
  table.enterprise-table tr:hover td { background: var(--bg-surface-hover); }

  /* BADGES DE ESTADO (SIN EMOJIS) */
  .badge-state {
    display: inline-block;
    padding: 2px 8px;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.4px;
    border-radius: 3px;
    text-transform: uppercase;
  }
  .badge-state.ok { background: #064e3b40; color: #34d399; border: 1px solid #05966955; }
  .badge-state.err { background: #7f1d1d40; color: #f87171; border: 1px solid #dc262655; }
  .badge-state.warn { background: #78350f40; color: #fbbf24; border: 1px solid #d9770655; }
  .code-text { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 11px; color: var(--brand-cyan); }

  /* SECCION SRE (METRICAS Y GRAFICAS) */
  .charts-grid { display: grid; grid-template-columns: 2fr 1fr; gap: 14px; margin-bottom: 20px; }
  .chart-card {
    background: var(--bg-surface);
    border: 1px solid var(--border-subtle);
    border-radius: 6px;
    padding: 14px;
  }
  .chart-card h4 { font-size: 11px; text-transform: uppercase; letter-spacing: 0.6px; color: var(--text-subtle); margin-bottom: 12px; }

  /* PANEL DE INYECCION DE FALLAS */
  .chaos-controls {
    background: var(--bg-surface);
    border: 1px solid var(--border-subtle);
    border-radius: 6px;
    padding: 16px;
    margin-bottom: 20px;
  }
  .chaos-controls-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 14px;
    padding-bottom: 10px;
    border-bottom: 1px solid var(--border-subtle);
  }
  .chaos-controls-header h3 { font-size: 13px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; }

  .traffic-config-bar {
    display: flex;
    gap: 14px;
    align-items: center;
    background: var(--bg-surface-elevated);
    padding: 10px 14px;
    border-radius: 4px;
    margin-bottom: 14px;
    flex-wrap: wrap;
  }
  .traffic-config-bar label { font-size: 11px; font-weight: 700; color: var(--text-muted); text-transform: uppercase; }
  select.select-dark {
    background: #090d16;
    color: #ffffff;
    border: 1px solid var(--border-strong);
    padding: 5px 10px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
  }

  .families-container { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; }
  .family-block {
    background: var(--bg-surface-elevated);
    border: 1px solid var(--border-subtle);
    border-radius: 4px;
    padding: 12px;
  }
  .family-block h5 {
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    color: var(--text-subtle);
    margin-bottom: 8px;
    border-bottom: 1px solid var(--border-subtle);
    padding-bottom: 4px;
  }
  .btn-group-chaos { display: flex; gap: 8px; flex-wrap: wrap; }
  .btn-fault {
    background: transparent;
    border: 1px solid var(--border-strong);
    color: var(--text-main);
    padding: 6px 10px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 700;
    cursor: pointer;
    transition: all 0.15s ease;
  }
  .btn-fault:hover { background: var(--bg-surface-hover); border-color: var(--brand-indigo); }
  .btn-fault.btn-active { background: var(--brand-indigo); border-color: var(--brand-indigo); color: #ffffff; }
</style>
</head>
<body>

<!-- NAVEGACIÓN SUPERIOR EMPRESARIAL -->
<nav class="enterprise-navbar">
  <div class="brand-group">
    <div class="brand-logo">SG</div>
    <div>
      <span class="brand-name">SupplyGrid Enterprise</span>
      <span class="brand-tag">B2B Procurement Network</span>
    </div>

    <!-- PESTAÑAS PRINCIPALES DEL SISTEMA -->
    <div class="nav-links">
      <button class="nav-tab-btn active" onclick="cambiarPestana('view-b2b-ops', this)">Operaciones B2B</button>
      <button class="nav-tab-btn" onclick="cambiarPestana('view-sre-chaos', this)">Consola SRE & Resiliencia</button>
      <button class="nav-tab-btn" onclick="cambiarPestana('view-causal-traces', this)">Auditoría de Trazas (trace_id)</button>
    </div>
  </div>

  <div class="nav-actions">
    <div class="status-pill normal" id="top-status-pill">
      <span class="status-dot"></span>
      <span id="top-status-text">RED: ESTABLE (BASELINE)</span>
    </div>
    <button class="btn-kill-switch" onclick="triggerKillSwitch()">Kill Switch SRE</button>
  </div>
</nav>

<div class="main-content">

  <!-- ============================================================================== -->
  <!-- VISTA 1: OPERACIONES B2B (PÁGINA PRINCIPAL)                                   -->
  <!-- ============================================================================== -->
  <div id="view-b2b-ops" class="tab-pane active">

    <!-- BANNER DE ESTADO DE LA RED DE SUMINISTRO -->
    <div class="banner-operational" id="banner-b2b">
      <div>
        <div style="font-weight:700; font-size:13px;" id="banner-b2b-title">SISTEMA B2B OPERANDO EN RÉGIMEN NORMAL</div>
        <div style="font-size:11px; color:var(--text-muted); margin-top:2px;" id="banner-b2b-desc">
          Contratos vigentes validados (Q1). SLAs logísticos dentro del umbral nominal (&lt; 2 ms).
        </div>
      </div>
      <div>
        <button class="btn-primary" onclick="crearPedidoManual()">Emitir Orden de Compra B2B</button>
      </div>
    </div>

    <!-- KPIS DE NEGOCIO B2B -->
    <div class="grid-summary">
      <div class="card-summary">
        <div class="title">Volumen Transaccionado Hoy</div>
        <div class="val" id="kpi-b2b-facturado" style="color:var(--status-ok);">$ 1,450.0 M COP</div>
        <div class="subtitle">Órdenes confirmadas en tiempo real</div>
      </div>
      <div class="card-summary">
        <div class="title">Cumplimiento de SLA Global</div>
        <div class="val" id="kpi-b2b-sla" style="color:var(--brand-cyan);">99.4 %</div>
        <div class="subtitle">Compromiso contractual (&gt;98.0%)</div>
      </div>
      <div class="card-summary">
        <div class="title">Proveedores en Red (Zipf)</div>
        <div class="val">5 Aliados</div>
        <div class="subtitle">40% volumen en 2 líderes certificados</div>
      </div>
      <div class="card-summary">
        <div class="title">Tasa de Despacho en ETA</div>
        <div class="val" id="kpi-b2b-ontime" style="color:#ffffff;">98.9 %</div>
        <div class="subtitle">Rutas terrestres y expreso activas</div>
      </div>
    </div>

    <!-- TABLA DE ÓRDENES DE COMPRA B2B EN VIVO -->
    <div class="panel-box">
      <div class="panel-header">
        <h3>Flujo de Órdenes de Compra y Despachos B2B en Tiempo Real</h3>
        <span style="font-size:11px; color:var(--text-subtle);">Correlación causal mediante trace_id</span>
      </div>
      <table class="enterprise-table">
        <thead>
          <tr>
            <th style="width: 100px;">Código Orden</th>
            <th style="width: 70px;">Hora</th>
            <th style="width: 240px;">Proveedor B2B</th>
            <th style="width: 220px;">Producto / Lote SKU</th>
            <th style="width: 140px;">Monto Total</th>
            <th style="width: 160px;">ETA Despacho</th>
            <th style="width: 160px;">Estado de Negocio</th>
            <th style="width: 80px;">Latencia</th>
            <th>trace_id</th>
          </tr>
        </thead>
        <tbody id="tbody-b2b-orders">
          <tr><td colspan="9" style="text-align:center; color:var(--text-subtle);">Sincronizando transacciones de compra...</td></tr>
        </tbody>
      </table>
    </div>

    <!-- DIRECTORIO DE PROVEEDORES HOMOLOGADOS -->
    <div class="panel-box">
      <div class="panel-header">
        <h3>Directorio de Proveedores Homologados (Concentración Zipf 40% en Líderes)</h3>
        <span style="font-size:11px; color:var(--text-subtle);">Verificación de Contrato Vigente (Q1)</span>
      </div>
      <table class="enterprise-table">
        <thead>
          <tr>
            <th>Código</th>
            <th>Razón Social</th>
            <th>NIT</th>
            <th>Ciudad Sede</th>
            <th>Calificación</th>
            <th>SLA Contractual</th>
            <th>Estado Contrato</th>
            <th>Latencia Media</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td><span class="code-text">PRV-001</span></td>
            <td><strong>Logística & Cargas del Café S.A.S.</strong> [Líder Zipf]</td>
            <td>900.123.456-1</td>
            <td>Medellín</td>
            <td style="font-weight:700;">4.9 / 5.0</td>
            <td style="color:var(--status-ok); font-weight:700;">99.2%</td>
            <td><span class="badge-state ok">VIGENTE</span></td>
            <td>0.4 ms</td>
          </tr>
          <tr>
            <td><span class="code-text">PRV-002</span></td>
            <td><strong>BioFarma Insumos Industriales S.A.</strong> [Líder Zipf]</td>
            <td>811.987.654-3</td>
            <td>Bogotá D.C.</td>
            <td style="font-weight:700;">5.0 / 5.0</td>
            <td style="color:var(--status-ok); font-weight:700;">98.8%</td>
            <td><span class="badge-state ok">VIGENTE</span></td>
            <td>0.6 ms</td>
          </tr>
          <tr>
            <td><span class="code-text">PRV-003</span></td>
            <td>Empaques & Polímeros de Occidente</td>
            <td>890.321.789-5</td>
            <td>Cali</td>
            <td>4.7 / 5.0</td>
            <td>97.5%</td>
            <td><span class="badge-state ok">VIGENTE</span></td>
            <td>0.8 ms</td>
          </tr>
          <tr>
            <td><span class="code-text">PRV-004</span></td>
            <td>Química Global del Caribe S.A.</td>
            <td>800.456.789-2</td>
            <td>Barranquilla</td>
            <td>4.8 / 5.0</td>
            <td>98.1%</td>
            <td><span class="badge-state ok">VIGENTE</span></td>
            <td>0.7 ms</td>
          </tr>
          <tr>
            <td><span class="code-text">PRV-005</span></td>
            <td>TechSupply Hardware B2B S.A.S.</td>
            <td>901.654.321-8</td>
            <td>Manizales</td>
            <td>4.9 / 5.0</td>
            <td>99.4%</td>
            <td><span class="badge-state ok">VIGENTE</span></td>
            <td>0.5 ms</td>
          </tr>
        </tbody>
      </table>
    </div>

  </div>

  <!-- ============================================================================== -->
  <!-- VISTA 2: CONSOLA SRE & RESILIENCIA (MÉTRICAS RED/USE E INYECCIÓN DE FALLAS)   -->
  <!-- ============================================================================== -->
  <div id="view-sre-chaos" class="tab-pane">

    <!-- METRICAS RED Y USE -->
    <div class="grid-summary">
      <div class="card-summary">
        <div class="title">Latencia p99 (Cola)</div>
        <div class="val" id="sre-val-p99" style="color:var(--status-warn);">0.0 ms</div>
        <div class="subtitle">p50: <span id="sre-val-p50">0.0</span> | p95: <span id="sre-val-p95">0.0</span> ms</div>
      </div>
      <div class="card-summary">
        <div class="title">Throughput (Tasa de Carga)</div>
        <div class="val" id="sre-val-tput" style="color:var(--brand-cyan);">0 ops/s</div>
        <div class="subtitle">Concurrencia multihilo activa</div>
      </div>
      <div class="card-summary">
        <div class="title">Tasa de Errores (RED)</div>
        <div class="val" id="sre-val-err" style="color:var(--status-ok);">0.0 %</div>
        <div class="subtitle">Total fallos interceptados: <span id="sre-val-err-count">0</span></div>
      </div>
      <div class="card-summary">
        <div class="title">Utilización CPU (USE)</div>
        <div class="val" id="sre-val-cpu" style="color:var(--text-main);">0.0 %</div>
        <div class="subtitle">Memoria RSS: <span id="sre-val-rss">0.0</span> MB</div>
      </div>
    </div>

    <!-- GRÁFICAS DE TELEMETRÍA EN VIVO -->
    <div class="charts-grid">
      <div class="chart-card">
        <h4>Latencia RED: p50 / p95 / p99 en Tiempo Real (ms)</h4>
        <div style="height: 210px;"><canvas id="chartLatency"></canvas></div>
      </div>
      <div class="chart-card">
        <h4>Throughput (ops/s) vs Tasa de Errores (%)</h4>
        <div style="height: 210px;"><canvas id="chartTputErr"></canvas></div>
      </div>
    </div>

    <!-- PANEL DE INYECCIÓN DE FALLAS EN CALIENTE -->
    <div class="chaos-controls">
      <div class="chaos-controls-header">
        <div>
          <h3>Inyección de Fallas Controladas y Mitigación de Arquitectura</h3>
          <div style="font-size:11px; color:var(--text-subtle); margin-top:2px;">
            Simulación de las 4 familias del catálogo de caos con auto-recuperación temporal
          </div>
        </div>
        <button class="btn-outline" onclick="resetCatalogo()">Restablecer Todo (Normal)</button>
      </div>

      <!-- BARRA DE CONFIGURACIÓN DE TRÁFICO -->
      <div class="traffic-config-bar">
        <label>Generador de Carga:</label>
        <button class="btn-outline" id="btn-toggle-traffic" onclick="toggleTrafico()">Pausar Tráfico</button>

        <label style="margin-left:10px;">Throughput:</label>
        <select id="sel-traffic-rps" class="select-dark" onchange="actualizarTrafico()">
          <option value="15">15 ops/s (Bajo)</option>
          <option value="40" selected>40 ops/s (Nominal Baseline)</option>
          <option value="80">80 ops/s (Alto)</option>
          <option value="150">150 ops/s (Estrés - Carga CPU ~30%)</option>
          <option value="300">300 ops/s (Saturación - Carga CPU ~60%)</option>
        </select>

        <label style="margin-left:14px;">Estrategia de Mitigación:</label>
        <select id="sel-mitigacion-strategy" class="select-dark" onchange="actualizarTrafico()">
          <option value="ninguna" selected>Ninguna (Llamada Directa)</option>
          <option value="cache">Caché TTL (60s en Memoria)</option>
          <option value="retry">3 Reintentos con Backoff Exponencial</option>
          <option value="breaker">Circuit Breaker (Fail-Fast)</option>
        </select>
      </div>

      <!-- LAS 4 FAMILIAS DE CAOS -->
      <div class="families-container">
        <!-- FAMILIA A -->
        <div class="family-block">
          <h5>Familia A: Latencia Inducida</h5>
          <div class="btn-group-chaos">
            <button class="btn-fault" onclick="activarFalla('A1_latencia_jitter', {mean_ms: 35.0, jitter_ms: 8.0}, 1.0)">A1: Latencia 35ms</button>
            <button class="btn-fault" onclick="activarFalla('A2_timeout_controlado', {timeout_ms: 12.0}, 1.0)">A2: Timeout 12ms</button>
          </div>
        </div>

        <!-- FAMILIA B -->
        <div class="family-block">
          <h5>Familia B: Base de Datos</h5>
          <div class="btn-group-chaos">
            <button class="btn-fault" onclick="activarFalla('B1_pool_agotado', {rejection_probability: 0.45}, 0.45)">B1: Pool Agotado (45%)</button>
            <button class="btn-fault" onclick="activarFalla('B2_bloqueo_recurso', {lock_wait_ms: 90.0, target_key: 'proveedor_1'}, 1.0)">B2: Bloqueo Recurso</button>
          </div>
        </div>

        <!-- FAMILIA C -->
        <div class="family-block">
          <h5>Familia C: Recursos (USE)</h5>
          <div class="btn-group-chaos">
            <button class="btn-fault" onclick="activarFalla('C1_saturacion_cpu', {intensity_pct: 70.0, duration_s: 15.0, threads: 4}, 1.0)">C1: CPU 70% (15s auto)</button>
            <button class="btn-fault" onclick="activarFalla('C2_presion_memoria', {size_mb: 250.0, duration_s: 15.0}, 1.0)">C2: RAM 250MB (15s)</button>
          </div>
        </div>

        <!-- FAMILIA D -->
        <div class="family-block">
          <h5>Familia D: Disponibilidad</h5>
          <div class="btn-group-chaos">
            <button class="btn-fault" onclick="activarFalla('D2_respuestas_5xx', {status_code: 503}, 0.35)">D2: Error 503 (35%)</button>
            <button class="btn-fault" onclick="activarFalla('D1_particion_red', {duration_s: 5.0}, 1.0)">D1: Corte Red (5s auto)</button>
          </div>
        </div>
      </div>
    </div>

  </div>

  <!-- ============================================================================== -->
  <!-- VISTA 3: AUDITORÍA CAUSAL DE TRAZAS (trace_id)                                -->
  <!-- ============================================================================== -->
  <div id="view-causal-traces" class="tab-pane">
    <div class="panel-box">
      <div class="panel-header">
        <h3>Stream de Trazas Causales y Excepciones Interceptadas (trace_id)</h3>
        <div>
          <label style="font-size:11px; color:var(--text-muted); cursor:pointer;">
            <input type="checkbox" id="chk-filter-errors" onchange="renderTrazas()"> Filtrar únicamente errores
          </label>
        </div>
      </div>
      <table class="enterprise-table">
        <thead>
          <tr>
            <th style="width: 80px;">Hora</th>
            <th style="width: 200px;">Módulo / Target</th>
            <th style="width: 90px;">Resultado</th>
            <th style="width: 90px;">Duración</th>
            <th style="width: 340px;">Detalle Técnico / Excepción</th>
            <th>trace_id de Correlación Causal</th>
          </tr>
        </thead>
        <tbody id="tbody-traces">
          <tr><td colspan="6" style="text-align:center; color:var(--text-subtle);">Esperando flujo de eventos...</td></tr>
        </tbody>
      </table>
    </div>
  </div>

</div>

<script>
let pestanaActiva = 'view-b2b-ops';
let traficoActivo = true;
let ultimasTrazasCache = [];

function cambiarPestana(vistaId, btn) {
  document.querySelectorAll('.tab-pane').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.nav-tab-btn').forEach(el => el.classList.remove('active'));
  document.getElementById(vistaId).classList.add('active');
  btn.classList.add('active');
  pestanaActiva = vistaId;
}

// Configuración de Chart.js con estilo sobrio y corporativo
const ctxLat = document.getElementById('chartLatency').getContext('2d');
const chartLat = new Chart(ctxLat, {
  type: 'line',
  data: {
    labels: [],
    datasets: [
      { label: 'p99 (ms)', data: [], borderColor: '#f59e0b', backgroundColor: 'transparent', tension: 0.1, borderWidth: 2, pointRadius: 0 },
      { label: 'p95 (ms)', data: [], borderColor: '#0284c7', backgroundColor: 'transparent', tension: 0.1, borderWidth: 1.5, pointRadius: 0 },
      { label: 'p50 (ms)', data: [], borderColor: '#10b981', backgroundColor: 'transparent', tension: 0.1, borderWidth: 1.5, pointRadius: 0 },
    ]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    scales: {
      x: { ticks: { color: '#64748b', maxTicksLimit: 7 }, grid: { color: '#1f293d' } },
      y: { ticks: { color: '#64748b' }, grid: { color: '#1f293d' }, beginAtZero: true }
    },
    plugins: { legend: { labels: { color: '#94a3b8', boxWidth: 10, font: { size: 10 } } } }
  }
});

const ctxTput = document.getElementById('chartTputErr').getContext('2d');
const chartTput = new Chart(ctxTput, {
  type: 'line',
  data: {
    labels: [],
    datasets: [
      { label: 'Throughput (ops/s)', data: [], borderColor: '#0284c7', backgroundColor: 'transparent', yAxisID: 'y', tension: 0.1, borderWidth: 2, pointRadius: 0 },
      { label: 'Errores (%)', data: [], borderColor: '#ef4444', backgroundColor: 'transparent', yAxisID: 'y1', tension: 0.1, borderWidth: 2, pointRadius: 0 },
    ]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    scales: {
      x: { ticks: { color: '#64748b', maxTicksLimit: 6 }, grid: { color: '#1f293d' } },
      y: { type: 'linear', position: 'left', ticks: { color: '#0284c7' }, grid: { color: '#1f293d' }, beginAtZero: true },
      y1: { type: 'linear', position: 'right', min: 0, max: 100, ticks: { color: '#ef4444' }, grid: { display: false } }
    },
    plugins: { legend: { labels: { color: '#94a3b8', boxWidth: 10, font: { size: 10 } } } }
  }
});

async function actualizarEstado() {
  try {
    const res = await fetch('/api/estado');
    const data = await res.json();

    const m = data.metricas;
    const c = data.caos;
    const hist = data.historico;

    // Actualizar métricas SRE
    document.getElementById('sre-val-p99').innerText = m.p99.toFixed(1) + ' ms';
    document.getElementById('sre-val-p50').innerText = m.p50.toFixed(1);
    document.getElementById('sre-val-p95').innerText = m.p95.toFixed(1);
    document.getElementById('sre-val-tput').innerText = m.throughput + ' ops/s';
    document.getElementById('sre-val-err').innerText = m.err_pct.toFixed(1) + ' %';
    document.getElementById('sre-val-err-count').innerText = m.total_errores;
    document.getElementById('sre-val-cpu').innerText = m.cpu_pct.toFixed(1) + ' %';
    document.getElementById('sre-val-rss').innerText = m.rss_mb.toFixed(1);

    // Indicador global en Navbar y Banners
    const topPill = document.getElementById('top-status-pill');
    const topText = document.getElementById('top-status-text');
    const bannerB2b = document.getElementById('banner-b2b');
    const b2bTitle = document.getElementById('banner-b2b-title');
    const b2bDesc = document.getElementById('banner-b2b-desc');

    if (c.kill_switch) {
      topPill.className = 'status-pill critical';
      topText.innerText = 'KILL SWITCH SRE ACTIVADO';
      bannerB2b.className = 'banner-operational error';
      b2bTitle.innerText = 'EMERGENCIA OPERACIONAL: KILL SWITCH ACTIVADO';
      b2bDesc.innerText = 'Todas las perturbaciones fueron abortadas de forma forzada por SRE.';
    } else if (c.fallas_activas && c.fallas_activas.length > 0) {
      topPill.className = 'status-pill degraded';
      topText.innerText = 'FALLA INYECTADA: ' + c.fallas_activas.join(', ');
      bannerB2b.className = 'banner-operational warning';
      b2bTitle.innerText = 'DEGRADACIÓN ACTIVA EN RED DE SUMINISTRO: ' + c.fallas_activas.join(', ');
      b2bDesc.innerText = 'El sistema está experimentando la perturbación inyectada. Revise la consola SRE para evaluar la resiliencia.';
    } else {
      topPill.className = 'status-pill normal';
      topText.innerText = 'RED: ESTABLE (BASELINE)';
      bannerB2b.className = 'banner-operational';
      b2bTitle.innerText = 'SISTEMA B2B OPERANDO EN RÉGIMEN NORMAL';
      b2bDesc.innerText = 'Contratos vigentes validados (Q1). SLAs logísticos dentro del umbral nominal (< 2 ms).';
    }

    // Actualizar KPIs de Negocio
    const slaActual = Math.max(0, 100.0 - m.err_pct).toFixed(1);
    const kpiSla = document.getElementById('kpi-b2b-sla');
    kpiSla.innerText = slaActual + ' %';
    kpiSla.style.color = (slaActual >= 98.0) ? 'var(--status-ok)' : ((slaActual >= 80.0) ? 'var(--status-warn)' : 'var(--status-err)');

    if (data.negocio && data.negocio.facturado_cop) {
      const mills = (data.negocio.facturado_cop / 1000000).toFixed(1);
      document.getElementById('kpi-b2b-facturado').innerText = `$ ${mills} M COP`;
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

    // Renderizar Pedidos B2B
    if (data.pedidos && data.pedidos.length > 0) {
      renderPedidosB2B(data.pedidos);
    }

    // Renderizar Trazas
    ultimasTrazasCache = data.trazas || [];
    renderTrazas();

  } catch (err) {
    console.error('Error al actualizar estado:', err);
  }
}

function renderPedidosB2B(pedidos) {
  const tbody = document.getElementById('tbody-b2b-orders');
  if (!tbody) return;
  let html = '';
  for (const p of pedidos) {
    const stateClass = p.exito ? 'ok' : 'err';
    html += `
      <tr>
        <td><strong class="code-text">${p.id}</strong></td>
        <td>${p.hora}</td>
        <td>${p.proveedor}</td>
        <td>${p.producto}</td>
        <td style="font-weight:700;">${p.monto_fmt}</td>
        <td style="color:var(--text-muted); font-size:11px;">${p.eta}</td>
        <td><span class="badge-state ${stateClass}">${p.estado}</span></td>
        <td>${p.dur_ms} ms</td>
        <td><span class="code-text" style="color:#64748b;">${p.trace_id}</span></td>
      </tr>
    `;
  }
  tbody.innerHTML = html;
}

function renderTrazas() {
  const tbody = document.getElementById('tbody-traces');
  if (!tbody) return;
  const soloErrores = document.getElementById('chk-filter-errors').checked;
  const lista = soloErrores ? ultimasTrazasCache.filter(t => t.resultado === 'error') : ultimasTrazasCache;

  if (lista.length === 0) {
    tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--text-subtle);">No hay eventos registrados con el criterio actual.</td></tr>';
    return;
  }

  let html = '';
  for (const t of lista) {
    const resClass = t.resultado === 'ok' ? 'badge-state ok' : 'badge-state err';
    html += `
      <tr>
        <td>${t.ts}</td>
        <td><span class="code-text">${t.target}</span></td>
        <td><span class="${resClass}">${t.resultado.toUpperCase()}</span></td>
        <td>${t.dur_ms} ms</td>
        <td style="color:${t.error ? 'var(--status-err)' : 'var(--text-muted)'}; font-size:11px;">${t.error || 'Operación completada con éxito'}</td>
        <td><span class="code-text" style="color:#64748b;">${t.trace_id}</span></td>
      </tr>
    `;
  }
  tbody.innerHTML = html;
}

async function activarFalla(faultId, params, trafficPct) {
  try {
    await fetch('/api/falla/activar', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        fault_id: faultId,
        parameters: params,
        traffic_pct: trafficPct !== undefined ? trafficPct : 1.0
      })
    });
    actualizarEstado();
  } catch (err) {
    alert('Error al activar falla: ' + err);
  }
}

async function triggerKillSwitch() {
  try {
    await fetch('/api/kill-switch', { method: 'POST' });
    actualizarEstado();
  } catch (err) {
    alert('Error al ejecutar Kill Switch: ' + err);
  }
}

async function resetCatalogo() {
  try {
    await fetch('/api/reset', { method: 'POST' });
    actualizarEstado();
  } catch (err) {
    alert('Error al restablecer catálogo: ' + err);
  }
}

async function toggleTrafico() {
  traficoActivo = !traficoActivo;
  const rps = parseInt(document.getElementById('sel-traffic-rps').value);
  const mitigacion = document.getElementById('sel-mitigacion-strategy').value;
  await fetch('/api/trafico', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ activo: traficoActivo, rps: rps, mitigacion: mitigacion })
  });

  const btn = document.getElementById('btn-toggle-traffic');
  btn.innerText = traficoActivo ? 'Pausar Tráfico' : 'Reanudar Tráfico';
}

async function actualizarTrafico() {
  if (!traficoActivo) return;
  const rps = parseInt(document.getElementById('sel-traffic-rps').value);
  const mitigacion = document.getElementById('sel-mitigacion-strategy').value;
  await fetch('/api/trafico', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ activo: true, rps: rps, mitigacion: mitigacion })
  });
}

async function crearPedidoManual() {
  try {
    await fetch('/api/pedido/crear', { method: 'POST' });
    actualizarEstado();
  } catch (err) {
    console.error('Error al emitir pedido:', err);
  }
}

setInterval(actualizarEstado, 1000);
actualizarEstado();
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
            obs.kill_switch("activado_desde_consola_sre" if nuevo_estado else "desactivado")
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
                if fault_id == "C1_saturacion_cpu":
                    self.generador.engine.start_cpu_burn(falla_obj)
                    dur_s = float(params.get("duration_s", 15.0))
                    def _auto_apagar_cpu():
                        time.sleep(dur_s)
                        self.manager.set_fault_state(fault_id, False)
                    threading.Thread(target=_auto_apagar_cpu, daemon=True).start()

                elif fault_id == "C2_presion_memoria":
                    self.generador.engine.apply_memory_pressure(falla_obj)
                    dur_s = float(params.get("duration_s", 15.0))
                    def _auto_apagar_ram():
                        time.sleep(dur_s)
                        self.manager.set_fault_state(fault_id, False)
                    threading.Thread(target=_auto_apagar_ram, daemon=True).start()

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
            self._responder_json(200, {"ok": True, "mensaje": "Estado restablecido a normal"})
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

    # Hilo recolector USE
    hilo_tel = threading.Thread(target=bucle_telemetria_fondo, daemon=True)
    hilo_tel.start()

    # Arrancar automáticamente el tráfico baseline (40 rps)
    generador.iniciar(rps=40, mitigacion="ninguna")

    server = socketserver.TCPServer(("0.0.0.0", puerto), TableroHttpHandler)

    url = f"http://localhost:{puerto}"
    print("=" * 75)
    print(" SUPPLYGRID ENTERPRISE · PLATAFORMA B2B Y CONSOLA SRE EN TIEMPO REAL")
    print("=" * 75)
    print(f" URL Servidor              : {url}")
    print(f" Régimen inicial           : Tráfico BASELINE ACTIVO (40 ops/s)")
    print(f" Concurrencia              : 3 Workers multihilo")
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
        print("Servidor finalizado.")


if __name__ == "__main__":
    puerto = int(sys.argv[1]) if len(sys.argv) > 1 else PUERTO_DEFECTO
    iniciar_tablero(puerto=puerto)
